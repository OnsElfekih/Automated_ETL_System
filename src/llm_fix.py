import requests

def fix_row_with_llm(row):
    prompt = f"""
Fix this sales record:
{row.to_dict()}

Return only corrected JSON.
"""

    response = requests.post(
        "http://localhost:11434/api/generate",
        json={
            "model": "mistral",
            "prompt": prompt,
            "stream": False
        }
    )

    return response.json()["response"]