import requests

# IGDB API credentials
# CLIENT_ID = "YOUR_CLIENT_ID"
# ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

# API URLs
BASE_URL = "https://api.igdb.com/v4"

# Headers for authentication
HEADERS = {
    "Client-ID": CLIENT_ID,
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}

def get_game_info(game_id):
    """Fetch game details including involved companies."""
    endpoint = f"{BASE_URL}/games"
    data = f"fields name, involved_companies; where id = {game_id};"
    response = requests.post(endpoint, headers=HEADERS, data=data)
    return response.json()

def get_company_id(involved_company_id):
    """Fetch the company ID from an involved company entry."""
    endpoint = f"{BASE_URL}/involved_companies"
    data = f"fields company; where id = {involved_company_id};"
    response = requests.post(endpoint, headers=HEADERS, data=data)
    return response.json()

def get_company_website(company_id):
    """Fetch company website details."""
    endpoint = f"{BASE_URL}/companies"
    data = f"fields name, websites.url; where id = {company_id};"
    response = requests.post(endpoint, headers=HEADERS, data=data)
    return response.json()

def find_company_website(game_id):
    """Retrieve the website of the company associated with a game."""
    game_info = get_game_info(game_id)
    
    if not game_info:
        return "Game not found."

    involved_companies = game_info[0].get("involved_companies", [])
    if not involved_companies:
        return "No company information available."

    company_info = get_company_id(involved_companies[0])
    if not company_info:
        return "Company ID not found."

    company_id = company_info[0].get("company")
    if not company_id:
        return "Company details missing."

    company_details = get_company_website(company_id)
    if not company_details or "websites" not in company_details[0]:
        return "Company website not found."

    return company_details[0]["websites"]

# Example usage
game_id = 12345  # Replace with the actual game ID
print(find_company_website(game_id))