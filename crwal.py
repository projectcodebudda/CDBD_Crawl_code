import requests
from bs4 import BeautifulSoup
import sys
import json
import io

def crawl_url(url, keyword, identifier, element=""):
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return json.dumps({"error": f"Error fetching the URL: {e}"})

    soup = BeautifulSoup(response.text, 'html.parser')

    results = []

    if element:
        elements = soup.find_all(element, string=lambda text: keyword in text if text else False)
        for el in elements:
            class_name = el.get('class', [])
            id_name = el.get('id', '')

            if class_name:
                class_name = ' '.join(class_name)
                related_elements = soup.find_all(class_=class_name)
            elif id_name:
                related_elements = [soup.find(id=id_name)]
            else:
                related_elements = [el]

            for rel_el in related_elements:
                text = rel_el.get_text(separator=' ', strip=True)
                text = ' '.join(text.split())
                results.append({
                    "url": url,
                    "identifier": identifier,
                    "data": text
                })
    else:
        elements = soup.find_all(string=lambda text: keyword in text if text else False)
        for el in elements:
            parent = el.find_parent()
            if parent:
                class_name = parent.get('class', [])
                id_name = parent.get('id', '')

                if class_name:
                    class_name = ' '.join(class_name)
                    related_elements = soup.find_all(class_=class_name)
                elif id_name:
                    related_elements = [soup.find(id=id_name)]
                else:
                    related_elements = [parent]

                for rel_el in related_elements:
                    text = rel_el.get_text(separator=' ', strip=True)
                    text = ' '.join(text.split())
                    results.append({
                        "url": url,
                        "identifier": identifier,
                        "data": text
                    })

    return json.dumps(results, ensure_ascii=False, indent=4)

def send_to_api(api_url, data):
    headers = {"Content-Type": "application/json"}
    print(f"Sending data to API: {api_url}")
    response = requests.post(api_url, headers=headers, data=data)
    print(f"API response code: {response.status_code}")
    print(f"API response text: {response.text}")
    return response

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: app.py <url> <keyword> <identifier> [<element>]")
        sys.exit(1)

    url = sys.argv[1]
    keyword = sys.argv[2]
    identifier = sys.argv[3]
    element = sys.argv[4] if len(sys.argv) > 4 else ""

    # Ensure output encoding is UTF-8
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

    # Perform the crawling
    result = crawl_url(url, keyword, identifier, element)

    # Send the result to the API
    api_url = "http://localhost:9999/api/v1/kafka/send"  
    send_to_api(api_url, result)
