import dlt
import requests
import time
import logging

def load_inventory_levels_gql(pipeline: dlt.Pipeline) -> None:
    """Robust inventoryLevels loader that never subscripts None."""
    import requests, time, logging

    shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
    access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
    if not shop_url or not access_token:
        logging.warning("⚠️ Missing Shopify credentials.")
        return

    gql_url = f"https://{shop_url.strip('https://').strip('http://').strip('/')}/admin/api/2024-01/graphql.json"
    headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

    query = """
    query GetInventoryLevels($first: Int!, $after: String) {
      inventoryLevels(first: $first, after: $after) {
        edges {
          cursor
          node {
            id
            createdAt
            updatedAt
            item { id sku tracked }
            location { id name }
            quantities(names:["available","on_hand","incoming","committed"]) {
              name
              quantity
            }
          }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
    """

    @dlt.resource(write_disposition="replace", name="inventory_levels")
    def inventory_levels_resource():
        after = None
        total = 0
        while True:
            try:
                r = requests.post(
                    gql_url,
                    headers=headers,
                    json={"query": query, "variables": {"first": 100, "after": after}},
                    timeout=60,
                )
                r.raise_for_status()
                j = r.json()
            except Exception as net_err:
                logging.warning(f"⚠️ Network error: {net_err}; stopping.")
                break

            inv_data = (j.get("data") or {}).get("inventoryLevels")
            if not inv_data:
                logging.warning("⚠️ Response contained no 'inventoryLevels'; stopping.")
                break

            edges = inv_data.get("edges") or []
            if not edges:
                logging.info("✅ No more edges; finished inventory stream.")
                break

            for e in edges:
                node = (e or {}).get("node")
                if node:
                    total += 1
                    yield node

            logging.info(f"📦 Streamed {len(edges)} records (total {total})")

            page = inv_data.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            after = page.get("endCursor")
            time.sleep(0.5)

        logging.info(f"✅ Completed inventory stream ({total} records).")

    try:
        pipeline.run(inventory_levels_resource())
    except Exception as e:
        logging.exception(f"❌ Failed to load inventory levels via DLT: {e}")


def load_pages(pipeline: dlt.Pipeline) -> None:
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        gql_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        query = """
        query GetPages($first: Int!, $after: String) {
          pages(first: $first, after: $after) {
            edges { node { id title handle createdAt updatedAt } }
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
                data = resp.json()["data"]["pages"]

                batch_count = len(data["edges"])
                total += batch_count

                for edge in data["edges"]:
                    yield edge["node"]

                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]

        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        gql_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        query = """
        query GetPages($first: Int!, $after: String) {
          pages(first: $first, after: $after) {
            edges {
              node {
                id
                title
                handle
                createdAt
                updatedAt
              }
            }
            pageInfo {
              hasNextPage
              endCursor
            }
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
                data = resp.json()["data"]["pages"]

                for edge in data["edges"]:
                    total += 1
                    yield edge["node"]

                print(f"📄 Loaded {len(data['edges'])} pages (total {total})")

                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]

            print(f"✅ Finished loading {total} pages")

        pipeline.run(pages_resource())

    except Exception as e:
        print(f"❌ Failed to load pages: {e}")


def load_pages_metafields(pipeline: dlt.Pipeline) -> None:
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        base_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        pages_url = f"{base_url}/pages.json?limit=250"
        page_ids = []
        while pages_url:
            resp = requests.get(pages_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()["pages"]
            page_ids.extend([p["id"] for p in data])

            link_header = resp.headers.get("Link", "")
            next_url = None
            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")
            pages_url = next_url

        @dlt.resource(write_disposition="replace", name="pages_metafields")
        def pages_metafields_resource():
            for pid in page_ids:
                url = f"{base_url}/pages/{pid}/metafields.json"
                resp = requests.get(url, headers=headers)
                resp.raise_for_status()
                for mf in resp.json()["metafields"]:
                    mf["page_id"] = pid
                    yield mf

        pipeline.run(pages_metafields_resource())

    except Exception as e:
        print(f"❌ Failed to load pages metafields: {e}")


def load_collections_metafields(pipeline: dlt.Pipeline) -> None:
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        base_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        url = f"{base_url}/custom_collections.json?limit=250"
        collection_ids = []
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        collections = resp.json()["custom_collections"]
        collection_ids.extend([c["id"] for c in collections])

        @dlt.resource(write_disposition="replace", name="collections_metafields")
        def collections_metafields_resource():
            for cid in collection_ids:
                resp = requests.get(f"{base_url}/collections/{cid}/metafields.json", headers=headers)
                resp.raise_for_status()
                for mf in resp.json()["metafields"]:
                    mf["collection_id"] = cid
                    yield mf

        pipeline.run(collections_metafields_resource())

    except Exception as e:
        print(f"❌ Failed to load collections metafields: {e}")


def load_blogs(pipeline: dlt.Pipeline) -> None:
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        gql_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

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
                data = resp.json()["data"]["blogs"]
                for edge in data["edges"]:
                    total += 1
                    yield edge["node"]
                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]
            print(f"✅ Loaded {total} blogs")

        pipeline.run(blogs_resource())

    except Exception as e:
        print(f"❌ Failed to load blogs: {e}")


def load_articles(pipeline: dlt.Pipeline) -> None:
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        gql_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01/graphql.json"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

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
                data = resp.json()["data"]["articles"]
                for edge in data["edges"]:
                    total += 1
                    yield edge["node"]
                if not data["pageInfo"]["hasNextPage"]:
                    break
                after = data["pageInfo"]["endCursor"]
            print(f"✅ Loaded {total} articles")

        pipeline.run(articles_resource())

    except Exception as e:
        print(f"❌ Failed to load articles: {e}")

def load_products_metafields(pipeline: dlt.Pipeline) -> None:
    """Loads product metafields with progress tracking and defensive timeouts."""
    try:
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            logging.warning("⚠️ Missing Shopify credentials; skipping product_metafields.")
            return

        base_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        # Step 1: Collect all product IDs
        products_url = f"{base_url}/products.json?limit=250"
        product_ids = []
        total_pages = 0

        while products_url:
            total_pages += 1
            resp = requests.get(products_url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json().get("products", [])
            product_ids.extend([p["id"] for p in data])

            link_header = resp.headers.get("Link", "")
            next_url = None
            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")
            products_url = next_url

            logging.info(f"🧩 Fetched {len(data)} products (page {total_pages}, total IDs: {len(product_ids)})")

        if not product_ids:
            logging.warning("⚠️ No products found; skipping metafield load.")
            return

        # Step 2: Fetch metafields per product
        start_time = time.time()
        total_metafields = 0
        total_products = len(product_ids)
        last_log_time = start_time

        @dlt.resource(write_disposition="replace", name="products_metafields")
        def products_metafields_resource():
            nonlocal total_metafields
            for i, product_id in enumerate(product_ids, start=1):
                try:
                    url = f"{base_url}/products/{product_id}/metafields.json"
                    resp = requests.get(url, headers=headers, timeout=20)
                    resp.raise_for_status()
                    metafields = resp.json().get("metafields", [])

                    for mf in metafields:
                        mf["product_id"] = product_id
                        total_metafields += 1
                        yield mf

                    # Periodic progress logs
                    if i % 25 == 0:
                        elapsed = round(time.time() - last_log_time, 1)
                        logging.info(
                            f"⏳ Processed {i}/{total_products} products "
                            f"({elapsed}s since last update, {total_metafields} metafields total)"
                        )
                        last_log_time = time.time()

                except requests.exceptions.RequestException as re:
                    logging.warning(f"⚠️ Request error for product {product_id}: {re}")
                    time.sleep(1)
                    continue

        pipeline.run(products_metafields_resource())

        total_time = round(time.time() - start_time, 2)
        logging.info(
            f"✅ Finished loading {total_metafields} metafields for {total_products} products in {total_time}s."
        )

    except Exception as e:
        logging.exception(f"❌ Failed to load product metafields: {e}")
        shop_url = dlt.config.get("sources.shopify_dlt.shop_url")
        access_token = dlt.config.get("sources.shopify_dlt.private_app_password")
        if not shop_url or not access_token:
            return

        base_url = f"https://{shop_url.replace('https://','').replace('http://','').strip('/')}/admin/api/2024-01"
        headers = {"X-Shopify-Access-Token": access_token, "Content-Type": "application/json"}

        # First, get all product IDs
        products_url = f"{base_url}/products.json?limit=250"
        product_ids = []
        
        while products_url:
            resp = requests.get(products_url, headers=headers)
            resp.raise_for_status()
            data = resp.json()["products"]
            product_ids.extend([p["id"] for p in data])

            # Handle pagination
            link_header = resp.headers.get("Link", "")
            next_url = None
            if 'rel="next"' in link_header:
                next_url = link_header.split(";")[0].strip("<>")
            products_url = next_url

        @dlt.resource(write_disposition="replace", name="products_metafields")
        def products_metafields_resource():
            total_metafields = 0
            for i, product_id in enumerate(product_ids):
                    url = f"{base_url}/products/{product_id}/metafields.json"
                    resp = requests.get(url, headers=headers)
                    resp.raise_for_status()
                    
                    metafields = resp.json().get("metafields", [])
                    for mf in metafields:
                        mf["product_id"] = product_id
                        total_metafields += 1
                        yield mf
                    

        pipeline.run(products_metafields_resource())