import dlt
import requests
import time
import logging


# ======================================================================
#  URL NORMALIZATION (no strip() bugs, fully safe)
# ======================================================================

def normalize_shop_url(raw: str) -> str:
    """
    Safely normalises a Shopify shop name or URL:
    - Removes protocol
    - Removes trailing slashes
    - Ensures .myshopify.com suffix
    """
    if not raw:
        return ""

    url = raw.strip()

    # Remove protocol ONLY when at the start
    if url.startswith("https://"):
        url = url[len("https://"):]
    elif url.startswith("http://"):
        url = url[len("http://"):]

    # Remove trailing slashes
    url = url.rstrip("/")

    # Ensure .myshopify.com
    if not url.endswith(".myshopify.com"):
        url = f"{url}.myshopify.com"

    return url


# ======================================================================
#  OAUTH (Shopify Admin API 2026 requirement)
# ======================================================================

def fetch_admin_access_token(shop_domain: str) -> str:
    """
    Exchanges client_id + client_secret for a short-lived Admin API access token.
    Required for Shopify Admin API after Jan 1, 2026.
    """

    client_id = dlt.config.get("sources.shopify_dlt.client_id")
    client_secret = dlt.config.get("sources.shopify_dlt.client_secret")

    if not client_id or not client_secret:
        logging.error("❌ Missing Shopify OAuth client credentials.")
        return ""

    url = f"https://{shop_domain}/admin/oauth/access_token"

    try:
        resp = requests.post(
            url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=30,
        )

        resp.raise_for_status()

        token = resp.json().get("access_token")
        if not token:
            logging.error("❌ No access_token returned from OAuth.")
            return ""

        return token

    except Exception as e:
        logging.exception(f"❌ Failed to fetch Admin access token: {e}")
        return ""


# ======================================================================
#  CENTRAL ENDPOINT BUILDER (uses OAuth token + normalized domain)
# ======================================================================

def get_shopify_endpoints():
    """
    Returns (gql_url, rest_base_url, headers) with fresh Admin API token.
    All loaders depend on this function.
    """

    raw_shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
    if not raw_shop_url:
        logging.warning("⚠️ Missing SHOP_URL; skipping.")
        return None, None, None

    domain = normalize_shop_url(raw_shop_url)

    # Get short-lived Admin API token
    access_token = fetch_admin_access_token(domain)
    if not access_token:
        return None, None, None

    gql_url = f"https://{domain}/admin/api/2024-01/graphql.json"
    rest_base = f"https://{domain}/admin/api/2024-01"

    headers = {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json"
    }

    return gql_url, rest_base, headers


# ======================================================================
#  LOAD INVENTORY LEVELS (GRAPHQL)
# ======================================================================

def load_inventory_levels_gql(pipeline: dlt.Pipeline) -> None:
    try:
        gql_url, rest_base, headers = get_shopify_endpoints()
        if not gql_url:
            return

        # Step 1: Get location
        loc_query = """
        query {
          locations(first: 1) {
            edges { node { id name } }
          }
        }
        """

        loc_resp = requests.post(gql_url, headers=headers, json={"query": loc_query})
        loc_resp.raise_for_status()

        loc_data = loc_resp.json()
        edges = (loc_data.get("data") or {}).get("locations", {}).get("edges", [])

        if not edges:
            logging.error("❌ No locations found.")
            return

        location = edges[0]["node"]
        loc_id = location["id"]
        loc_name = location["name"]

        logging.info(f"🏬 Using location: {loc_name} ({loc_id})")

        # Step 2: Query inventory
        query = """
        query GetInventoryLevels($locationId: ID!, $first: Int!, $after: String) {
          location(id: $locationId) {
            inventoryLevels(first: $first, after: $after) {
              edges {
                node {
                  id
                  quantities(names: ["available", "incoming", "committed", "damaged", "on_hand"]) {
                    name quantity
                  }
                  item { id sku }
                }
              }
              pageInfo { hasNextPage endCursor }
            }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="inventory_levels")
        def inventory_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={
                        "query": query,
                        "variables": {
                            "locationId": loc_id,
                            "first": 100,
                            "after": after
                        },
                    },
                )
                resp.raise_for_status()

                data = resp.json().get("data", {}).get("location", {})
                inv = data.get("inventoryLevels", {})
                edges = inv.get("edges", [])

                if not edges:
                    break

                for edge in edges:
                    node = edge.get("node")
                    if node:
                        node["location_id"] = loc_id
                        node["location_name"] = loc_name
                        total += 1
                        yield node

                page_info = inv.get("pageInfo", {})
                if not page_info.get("hasNextPage"):
                    break

                after = page_info.get("endCursor")

            logging.info(f"✅ Loaded {total} inventory levels.")

        pipeline.run(inventory_resource())

    except Exception as e:
        logging.exception(f"❌ Failed to load inventory levels: {e}")


# ======================================================================
#  LOAD PAGES (GRAPHQL)
# ======================================================================

def load_pages(pipeline: dlt.Pipeline) -> None:
    try:
        gql_url, rest_base, headers = get_shopify_endpoints()
        if not gql_url:
            return

        query = """
        query GetPages($first: Int!, $after: String) {
          pages(first: $first, after: $after) {
            edges {
              node { id title handle createdAt updatedAt }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="pages")
        def pages_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                )
                resp.raise_for_status()

                pages = resp.json()["data"]["pages"]
                edges = pages["edges"]

                for edge in edges:
                    total += 1
                    yield edge["node"]

                if not pages["pageInfo"]["hasNextPage"]:
                    break

                after = pages["pageInfo"]["endCursor"]

            print(f"✅ Loaded {total} pages")

        pipeline.run(pages_resource())

    except Exception as e:
        print(f"❌ Failed to load pages: {e}")


# ======================================================================
#  LOAD PAGE METAFIELDS (REST)
# ======================================================================

def load_pages_metafields(pipeline: dlt.Pipeline) -> None:
    try:
        gql_url, rest_base, headers = get_shopify_endpoints()
        if not rest_base:
            return

        pages_url = f"{rest_base}/pages.json?limit=250"
        page_ids = []

        while pages_url:
            resp = requests.get(pages_url, headers=headers)
            resp.raise_for_status()

            data = resp.json()["pages"]
            page_ids.extend([p["id"] for p in data])

            # Pagination
            link_header = resp.headers.get("Link", "")
            next_url = None
            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")

            pages_url = next_url

        @dlt.resource(write_disposition="replace", name="pages_metafields")
        def pages_mf_resource():
            for pid in page_ids:
                url = f"{rest_base}/pages/{pid}/metafields.json"
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()

                for mf in resp.json()["metafields"]:
                    mf["page_id"] = pid
                    yield mf

        pipeline.run(pages_mf_resource())

    except Exception as e:
        print(f"❌ Failed to load pages metafields: {e}")


# ======================================================================
#  LOAD COLLECTION METAFIELDS (REST)
# ======================================================================

def load_collections_metafields(pipeline: dlt.Pipeline) -> None:
    try:
        gql_url, rest_base, headers = get_shopify_endpoints()
        if not rest_base:
            return

        url = f"{rest_base}/custom_collections.json?limit=250"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()

        collections = resp.json()["custom_collections"]
        collection_ids = [c["id"] for c in collections]

        @dlt.resource(write_disposition="replace", name="collections_metafields")
        def col_mf_resource():
            for cid in collection_ids:
                url = f"{rest_base}/collections/{cid}/metafields.json"
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()

                for mf in resp.json()["metafields"]:
                    mf["collection_id"] = cid
                    yield mf

        pipeline.run(col_mf_resource())

    except Exception as e:
        print(f"❌ Failed to load collections metafields: {e}")


# ======================================================================
#  LOAD BLOGS (GRAPHQL)
# ======================================================================

def load_blogs(pipeline: dlt.Pipeline) -> None:
    try:
        gql_url, rest_base, headers = get_shopify_endpoints()
        if not gql_url:
            return

        query = """
        query GetBlogs($first: Int!, $after: String) {
          blogs(first: $first, after: $after) {
            edges {
              node { id title handle createdAt updatedAt }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="blogs")
        def blogs_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                )
                resp.raise_for_status()

                blogs = resp.json()["data"]["blogs"]
                edges = blogs["edges"]

                for edge in edges:
                    total += 1
                    yield edge["node"]

                if not blogs["pageInfo"]["hasNextPage"]:
                    break

                after = blogs["pageInfo"]["endCursor"]

            print(f"✅ Loaded {total} blogs")

        pipeline.run(blogs_resource())

    except Exception as e:
        print(f"❌ Failed to load blogs: {e}")


# ======================================================================
#  LOAD ARTICLES (GRAPHQL)
# ======================================================================

def load_articles(pipeline: dlt.Pipeline) -> None:
    try:
        gql_url, rest_base, headers = get_shopify_endpoints()
        if not gql_url:
            return

        query = """
        query GetArticles($first: Int!, $after: String) {
          articles(first: $first, after: $after) {
            edges {
              node { id title handle createdAt updatedAt }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """

        @dlt.resource(write_disposition="replace", name="articles")
        def articles_resource():
            after = None
            total = 0

            while True:
                resp = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                )
                resp.raise_for_status()

                arts = resp.json()["data"]["articles"]
                edges = arts["edges"]

                for edge in edges:
                    total += 1
                    yield edge["node"]

                if not arts["pageInfo"]["hasNextPage"]:
                    break

                after = arts["pageInfo"]["endCursor"]

            print(f"✅ Loaded {total} articles")

        pipeline.run(articles_resource())

    except Exception as e:
        print(f"❌ Failed to load articles: {e}")


# ======================================================================
#  LOAD PRODUCT METAFIELDS (REST)
# ======================================================================

def load_products_metafields(pipeline: dlt.Pipeline) -> None:
    try:
        gql_url, rest_base, headers = get_shopify_endpoints()
        if not rest_base:
            return

        # STEP 1: gather product IDs
        products_url = f"{rest_base}/products.json?limit=250"
        product_ids = []

        while products_url:
            resp = requests.get(products_url, headers=headers)
            resp.raise_for_status()

            data = resp.json().get("products", [])
            product_ids.extend([p["id"] for p in data])

            # Pagination
            link_header = resp.headers.get("Link", "")
            next_url = None

            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")

            products_url = next_url

        if not product_ids:
            logging.warning("⚠️ No products found.")
            return

        start_time = time.time()
        total_metafields = 0

        @dlt.resource(write_disposition="replace", name="products_metafields")
        def products_mf_resource():
            nonlocal total_metafields

            for product_id in product_ids:
                try:
                    url = f"{rest_base}/products/{product_id}/metafields.json"
                    resp = requests.get(url, headers=headers, timeout=20)
                    resp.raise_for_status()

                    metafields = resp.json().get("metafields", [])

                    for mf in metafields:
                        mf["product_id"] = product_id
                        total_metafields += 1
                        yield mf

                except requests.exceptions.RequestException as re:
                    logging.warning(f"⚠️ Request error for product {product_id}: {re}")
                    time.sleep(1)
                    continue

        pipeline.run(products_mf_resource())

        elapsed = round(time.time() - start_time, 2)
        logging.info(
            f"✅ Loaded {total_metafields} product metafields "
            f"in {elapsed}s."
        )

    except Exception as e:
        logging.exception(f"❌ Failed to load product metafields: {e}")
