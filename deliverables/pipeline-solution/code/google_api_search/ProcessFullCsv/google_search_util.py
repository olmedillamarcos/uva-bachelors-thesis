import requests
import json
import urllib.parse
import os
import logging 

def linkedin_profile_search(query: str, university_names: list = None, course_names: list = None, api_key: str = None, search_engine_id: str = None):
    """
    Find the URL of the best matching Linkedin profile for a person using Google Custom Search.

    Args:
        query: The person's full name whose profile you want to find on Linkedin.
        university_names: An optional list of university names the person might have studied at.
        course_names: An optional list of course or degree names the person might have completed.
        api_key: The Google Custom Search API Key.
        search_engine_id: The Google Custom Search Engine ID.

    Returns:
        A single URL string if a relevant result is found, otherwise None.

    Raises:
        ValueError: If API keys are missing.
        requests.exceptions.RequestException: If there's an issue with the API request.
        json.JSONDecodeError: If the API response cannot be parsed as JSON.
        KeyError: If the expected keys are not found in the API response.
        Exception: For other unexpected errors.
    """

    # Ensure API keys are provided
    if not api_key or not search_engine_id:
        raise ValueError("Google API Key and Search Engine ID must be provided.")

    url_query = "https://www.googleapis.com/customsearch/v1"

    # --- Constructing the search query ---
    search_terms = [f'"{query}"'] # Exact phrase for the person's name

    if university_names:
        university_clauses = [f'"{name}"' for name in university_names]
        search_terms.append(f'({" OR ".join(university_clauses)})')

    if course_names:
        course_clauses = [f'"{name}"' for name in course_names]
        search_terms.append(f'({" OR ".join(course_clauses)})')

    q = " AND ".join(search_terms)
    q += " site:linkedin.com/in/" # Restrict search to LinkedIn profiles

    params = {
        "key": api_key,
        "cx": search_engine_id,
        "q": q,
        "num": 1  # Request only the single best result
    }

    logging.info(f"Executing search for: {query}")

    try:
        response = requests.get(url_query, params=params)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        res = response.json()

        # Check for search results and filter for LinkedIn person profiles
        # We only need the first item if it exists and matches the criteria
        if 'items' in res and res['items']:
            first_item = res['items'][0]
            formatted_url = first_item.get("formattedUrl")
            if formatted_url and "/in/" in formatted_url:
                logging.info(f"Found best match for '{query}': {formatted_url}")
                return formatted_url
        
        logging.info(f"No relevant LinkedIn profile found for: {query}")
        return None # No relevant results found

    except requests.exceptions.RequestException as e:
        logging.error(f"API request error for '{query}': {e}")
        raise
    except json.JSONDecodeError:
        logging.error(f"JSON decoding error for API response for '{query}'. Response: {response.text[:200]}...")
        raise ValueError("Invalid JSON response from API.")
    except (KeyError, TypeError) as e:
        logging.error(f"API response structure error for '{query}': {e}. Response: {res}")
        raise KeyError("Unexpected API response structure.")
    except Exception as e:
        logging.error(f"An unexpected error occurred for '{query}': {e}", exc_info=True)
        raise

