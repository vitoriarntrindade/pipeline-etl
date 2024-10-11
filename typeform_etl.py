import os
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def fetch_responses(api_token, form_id):
    """Fetch responses from Typeform API."""
    url = f"https://api.typeform.com/forms/{form_id}/responses"
    headers = {"Authorization": f"Bearer {api_token}"}
    params = {'page_size': 100}

    try:
        response = requests.get(url=url, headers=headers, params=params)
        response.raise_for_status()  
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching responses: {e}")
        return None

def format_responses(data):
    """Format the API response into a list of dictionaries."""
    if not data or 'items' not in data:
        return []

    formatted_responses = []
    for resp in data['items']:
        formatted_response = {
            'submitted_at': resp['submitted_at'],
            'response_id': resp['response_id']
        }

        for answer in resp.get('answers', []):
            question_id = answer['field']['id']
            question_type = answer['type']

            if question_type in ['text', 'number', 'date']:
                formatted_response[question_id] = answer.get(question_type)

        formatted_responses.append(formatted_response)

    return formatted_responses

def save_to_sql(data, database_url):
    """Save the formatted data to a SQL database."""
    engine = create_engine(database_url)
    df = pd.DataFrame(data)

    # Salva os dados em uma tabela chamada 'responses'
    df.to_sql('responses', con=engine, if_exists='append', index=False)

def main():
    """Main function to orchestrate the ETL process."""
    # Step 1: Fetch responses from the API
    api_token = os.getenv('API_TOKEN')
    form_id = os.getenv('FORM_ID')
    database_url = os.getenv('DATABASE_URL')

    data = fetch_responses(api_token, form_id)

    # Step 2: Format the responses
    formatted_data = format_responses(data)

    # Step 3: Save the formatted data to the SQL database
    if formatted_data:
        save_to_sql(formatted_data, database_url)
        print(f"Data saved to database with {len(formatted_data)} entries.")
    else:
        print("No data to save.")

if __name__ == "__main__":
    # Chama a função principal com os parâmetros necessários
    main()
