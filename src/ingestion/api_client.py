import requests

def fetch_api_data(url, params = None, headers = None):
    """
    Fetch JSON data from an API endpoint.
    """

    response = requests.get(
        url,
        params = params,
        headers = headers,
        timeout = 30
    )

    response.raise_for_status()

    return response.json()

def fetch_crm_customers():
    """
    Fetch customer records from the CRM API
    and transform them into the CRM schema
    used by the pipeline.
    """

    url = "https://jsonplaceholder.typicode.com/users"

    records = fetch_api_data(url)

    customers = []

    for record in records:
        customers.append({
            "customer_id": f"C{record['id']:03d}",
            "name": record.get("name"),
            "email": record.get("email"),
            "company": record.get("company", {}).get("name"),
            "phone": record.get("phone")
        })

    return customers

def fetch_marketing_customers():
    """
    Fetch customer records from the marketing API.
    """

    url = "http://jsonplaceholder.typicode.com/users"

    records = fetch_api_data(url)

    customers = []

    for record in records:
        customers.append({
            "lead_id": f"L{record['id']:03d}",
            "name": record.get("name"),
            "email": record.get("email"),
            "company": record.get("company", {}).get("name"),
            "phone": record.get("phone")
        })

    return customers

def fetch_sales_customers():
    """
    Fetch customer records from the sales API.
    """

    url = "https://jsonplaceholder.typicode.com/users"

    records = fetch_api_data(url)

    customers = []

    for record in records:
        customers.append({
            "sales_id": f"S{record['id']:03d}",
            "name": record.get("name"),
            "email": record.get("email"),
            "company": record.get("company", {}).get("name"),
            "phone": record.get("phone")
        })

    return customers