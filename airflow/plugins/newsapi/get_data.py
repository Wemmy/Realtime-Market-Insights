import requests

class NewsAPI:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://newsapi.org/v2"

    def fetch_news(self, query, from_date=None, to_date=None, language="en"):
        """Fetch news articles based on query and optional date range."""
        url = f"{self.base_url}/everything"
        params = {
            'q': query,
            'apiKey': self.api_key,
            'from': from_date,
            'to': to_date,
            'language': language
        }
        # Filter out None values in parameters
        params = {k: v for k, v in params.items() if v is not None}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raises HTTPError for bad responses
            return response.json()  # Return the parsed JSON data
        except requests.RequestException as e:
            print(f"Error fetching news: {e}")
            return None