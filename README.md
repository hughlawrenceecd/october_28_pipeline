 <p align="center">
  <img src="https://capsule-render.vercel.app/api?text=Shopify%20Data%20Pipeline&animation=fadeIn&type=waving&color=gradient&height=100"/>
</p>

This guide explains how to install, configure, and deploy the **Shopify DLT pipeline**, which extracts data from the Shopify Admin and Partner APIs and loads it into a PostgreSQL database hosted on **DigitalOcean**.  
It also covers **automated runs via GitHub Actions** and **DigitalOcean self-hosted runners**.

---
# How it works

The pipeline has 2 main components:

## 1. Data transfer

- A python script fetches data from Shopify using their API
- Data is stores in either:
	- PostgreSQL (production, hosted within a database cluster in digital ocean)
	- DuckDB (Local testing)
## 2. Scheduled Cron Job

- A GitHub Actions workflow runs the python script every 5 mins **
- Hosted on a self-hosted GitHub runner inside a Digital Ocean droplet, which is a linux server
- Uses cron syntax and configuration for the Shopify API -> DB inside a YAML file

** *While the workflow is configured to run every five minutes, the actual execution interval can vary due to GitHub Actions’ scheduling limitations for self-hosted runners. This behaviour is inherent to the platform and cannot be avoided within the current stack.*
---

---

## 1. Prerequisites (if running locally)

### Python environment

Ensure you are using **Python 3.9 – 3.13** and have **pip** installed.

```bash
python --version
pip --version
```

If not, install or update Python.

#### macOS (Homebrew)
```bash
brew update
brew install python@3.10
pip install uv
```

#### Ubuntu
```bash
sudo apt update
sudo apt install python3.10 python3-pip -y
pip install uv
```

---

## 2. Set up your virtual environment (if using locally)

Create and activate a clean, isolated Python environment:

```bash
uv venv --python 3.10
source .venv/bin/activate
```

---

## 3. Install DLT and project dependencies (if using locally)

Install the latest version of **dlt** and all project dependencies:

```bash
uv pip install -U dlt
uv pip install -r requirements.txt
```

---

## 4. Shopify API setup

### Create a Custom App (Admin API)

1. Log in to your Shopify Admin.
2. Go to **Settings ⚙️ → Apps and sales channels → Develop apps**.
3. Click **Create an app** → enter app details.
4. Under **Configuration**, click **Configure** under *Admin API integration*.
5. Enable **read access** for these scopes (and any others that may be required):
   - `read_products`
   - `read_product_listings`
   - `read_orders`
   - `read_draft_orders`
   - `read_customers`
   - `read_inventory`
   - `read_locations`
   - `read_metaobjects`
   - `read_metaobjct_definitions`
   - `read_content`
  
6. Click **Save** → **Install app → Confirm**.
7. Click **Reveal token** and **copy** the Admin API token (you’ll only see it once).

---

### Create a Partner API token (if using locally)

1. Log in to [Shopify Partners](https://partners.shopify.com).
2. Go to **Settings ⚙️ → Partner API client → Manage Partner API clients**.
3. Create a new API client with appropriate permissions.
4. Save → click **Show token** → copy and store securely.

---

## 5. Configure DLT credentials (if using locally)

Inside your project directory, locate or create a `.dlt/` folder with two files:

### `.dlt/secrets.toml`

Stores **sensitive API tokens** and **database credentials**. Never commit this file.

```toml
# Shopify credentials
[sources.shopify_dlt]
private_app_password = "your_admin_api_token_here"
access_token = "your_partner_api_token_here"

# PostgreSQL credentials
[destination.postgres.credentials]
database = "shopify_dlt_data"
username = "doadmin"
password = "your_postgres_password"
host     = "your-do-cluster.h.db.ondigitalocean.com"
port     = 25060
```

---

### `.dlt/config.toml`

Stores non-sensitive configuration values:

```toml
[sources.shopify_dlt]
shop_url = "https://your-shop-name.myshopify.com"
organization_id = "1234567"
```

---

## 6. GitHub Actions Workflow (Cron Job)

This workflow runs your pipeline on a schedule using your self-hosted runner.
Ensure the DESTINATION__POSTGRES credentials match the credentials in your database, found in this area:

<img width="640" height="424" alt="Screenshot 2025-10-29 at 09 58 51" src="https://github.com/user-attachments/assets/1e04867f-92c4-4767-a1b5-2a2206be387d" />


Create a file at `.github/workflows/shopify_pipeline.yml`:

```yaml
name: Run Shopify DLT Pipeline

on:
  schedule:
    - cron: "*/5 * * * *"  # Runs every 5 minutes
  workflow_dispatch:        # Allow manual runs

env:
  DESTINATION__POSTGRES__CREDENTIALS__DATABASE: database name
  DESTINATION__POSTGRES__CREDENTIALS__USERNAME: doadmin
  DESTINATION__POSTGRES__CREDENTIALS__HOST: your-db-host.ondigitalocean.com
  DESTINATION__POSTGRES__CREDENTIALS__PORT: "25060"
  SOURCES__SHOPIFY_DLT__SHOP_URL: "https://your-shop-name.myshopify.com"
  SOURCES__SHOPIFY_DLT__CLIENT_ID: ${{ secrets.SOURCES__SHOPIFY_DLT__CLIENT_ID }}
  SOURCES__SHOPIFY_DLT__CLIENT_SECRET: ${{ secrets.SOURCES__SHOPIFY_DLT__CLIENT_SECRET }}
  DESTINATION__POSTGRES__CREDENTIALS__PASSWORD: ${{ secrets.DESTINATION__POSTGRES__CREDENTIALS__PASSWORD }}

jobs:
  run_pipeline:
    runs-on: [self-hosted, Linux, X64, staging, shopify]
    steps:
      - name: Checkout code
        uses: actions/checkout@v3

      - name: Install Python venv
        run: |
          sudo apt-get update
          sudo apt-get install -y python3.13-venv

      - name: Set up Python and install dependencies
        run: |
          python3 -m venv venv
          source venv/bin/activate
          pip install --upgrade pip
          pip install -r requirements.txt

      - name: Run DLT pipeline
        run: |
          source venv/bin/activate
          python shopify_dlt_pipeline.py
```

---


## 7. Configure GitHub Secrets

To allow **GitHub Actions** to access your Shopify and database credentials, add the following secrets.

Navigate to your repo →  
**Settings → Secrets and variables → Actions → New repository secret**

| Secret name | Description |
|--------------|-------------|
| `SOURCES__SHOPIFY_DLT__PRIVATE_APP_PASSWORD` | Shopify Admin API token |
| `DESTINATION__POSTGRES__CREDENTIALS__PASSWORD` | DigitalOcean PostgreSQL password |

These secrets are injected into your workflow YAML file for cron-based automation.

**Add these same credentials to the environment secrets too.** Go to 
**Settings → Secrets and Variables → Actions → Manage envornment secrets → Create and name your new envornment**

Add the same secrets

| Secret name | Description |
|--------------|-------------|
| `SOURCES__SHOPIFY_DLT__PRIVATE_APP_PASSWORD` | Shopify Admin API token |
| `DESTINATION__POSTGRES__CREDENTIALS__PASSWORD` | DigitalOcean PostgreSQL password |

---

## 8. Digital Ocean Droplet Setup

---

### 8.1 Install Python and Pip on droplet

Before creating any users or configuraing the runner, ensure the droplet has root level Python and Pip installed


```bash
sudo apt update
sudo apt install python3
sudo apt install python3-pip
sudo apt install python3-venv
sudo add-apt-repository ppa:deadsnakes/ppa -y

```

You can verify installation with:

```bash
python3 --version
pip3 --version
```
---

### 8.2 Add droplet as a trusted source for the database

To allow the new dorplet to connect to the database it must be addeda as a trsuted source

1. Login to digitalOcean
2. Navigate to **Database -> PostgreSQL Cluser**
3. Under **Network Access -> Trusted Sources**, click **Add Trusted Sources**
4. Choose the droplet you created earlier (or manually add it's IP Address)
5. Save changes

---

### 8.3 Create and Configure a new user

Inside your droplet:

```bash
# Create a new user
adduser new_user

# Add to sudo group
usermod -aG sudo new_user

# Switch to the new user
su - new_user

# Allow passwordless sudo
sudo visudo
```

Add these lines **at the bottom** of the file:

```
new_user ALL=(ALL) NOPASSWD:ALL
new_user ALL=(ALL) NOPASSWD:/usr/bin/apt-get
```

Save and exit (`Ctrl + X`)

---

### 8.4 Install Github Actions Runner

1. In your GitHub repo, go to **Settings → Actions → Runners → New self-hosted runner**.
2. Click enter during the config stage with the github runner to select the default optinos for labels, folder placement etc.
3. Choose **Linux** and follow the displayed commands **until you get to the `./run.sh` command**
4. **Do not run `./run.sh` command! This takes over the console and means you can't run a systems check, or change users in the droplet without stopping the runner.**
5. Instead, proceed to the step below

---

### 9. Configure and start the runner

```bash

# Install and start as a service
sudo ./svc.sh install
sudo ./svc.sh start

# (Optional) Check status
sudo systemctl status actions.runner.*

# (Optional) Test DB connectivity
nc -zv your-db-host.ondigitalocean.com 25060
```

---

### How to run it

1. Start your self-hosted runner on the server (`sudo ./svc.sh start`).
2. Go to GitHub → **Actions → Run workflow manually**.
3. The pipeline will execute using your DigitalOcean server.
4. Logs will stream live in the Actions tab.

---

## 10. Choosing the Right Pipeline Function

The pipeline script contains **two functions** for different stages of data loading.

### `incremental_load_with_backloading()`
**Use this first.**  
This function performs a *historical backfill* of your data in weekly chunks (e.g. orders, customers, products), then switches to incremental updates.

- Ideal for **first-time setups**.
- Safely handles large datasets over time.
- Automatically backfills from a fixed start date.

Example:
```python
incremental_load_with_backloading()
```

---

### `load_all_resources(resources, start_date=...)`
**Use this after backfill is complete.**  
This function performs regular incremental updates on all key resources (orders, customers, products, etc.) — much faster and lighter.

Example:
```python
load_all_resources(["orders", "customers", "products"], start_date="2025-10-10")
```

**Workflow tip:**  
Run `incremental_load_with_backloading()` once to initialize your historical data,  
then comment it out and switch to `load_all_resources()` for ongoing scheduled runs.

---

## 11. Troubleshooting

| Issue | Likely Cause | Fix |
|-------|---------------|-----|
| `no password supplied` | Missing DB password | Ensure GitHub secret `DESTINATION__POSTGRES__CREDENTIALS__PASSWORD` is set |
| `inventoryLevels doesn't exist` | Missing `read_inventory` or `read_locations` scopes | Re-enable these in the app config |
| Runner inactive | Labels or service misconfigured | Check labels match YAML; restart via `sudo ./svc.sh start` |
| Database connection refused | IP not trusted | Add Droplet IP as a trusted source in DigitalOcean |
| Workflow won’t start | Actions disabled | Go to Actions → Workflow → “Enable workflow” |

---

## 12. Maintenance Tips

-  Never commit `.dlt/secrets.toml`
-  Rotate Shopify tokens periodically
-  Check Action logs for “missing scope” warnings
-  Update dependencies occasionally:
  ```bash
  pip install -U dlt
  ```
-  Verify your DigitalOcean database “Trusted Sources” list includes your runner’s IP

For more information, see the [Wiki](https://github.com/hughlawrenceecd/october_28_pipeline/wiki)

---
