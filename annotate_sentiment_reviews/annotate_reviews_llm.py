import json
import pandas as pd
import openai
import json 

client = openai.OpenAI(api_key="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")

def annotate_with_llm(text):
    prompt = f"""
You are a fashion expert annotator. Given the following fashion show review, do two things:
1. Label the sentiment as either "very positive" (1) or "somewhat negative" (0). Consider the tone, word choice, and overall impression.
2. If the annotation is 0, extract the phrases that convey a negative nuance.

Respond in JSON format with the following fields:
- annotation: 1 or 0
- explanation: short quote or phrase that shows the negative nuance (only if annotation is 0)

Example:
Review: "It was in a romantic, youthful mood at first, updating prairie themes that have been so popular. But at times, things got just a little too girly — it’s hard to imagine anyone past 16 wearing a Little Red Riding Hood smock with gathered short sleeves."
Response:
{{
  "annotation": 0,
  "explanation": "it’s hard to imagine anyone past 16 wearing a Little Red Riding Hood smock"
}}

Now annotate this review:
{text}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",  # or "gpt-3.5-turbo"
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )

        content = response.choices[0].message.content.strip()

        # Remove code block formatting if present
        if content.startswith("```"):
            content = content.split("```")[1].replace("json", "").strip()

        result = json.loads(content)
        return pd.Series({
            "annotation": result.get("annotation"),
            "explanation": result.get("explanation") or ""
        })

    except Exception as e:
        print(f"[ERROR] Failed to parse LLM response for input: {text[:80]}... -> {e}")
        print(f"Raw content: {content if 'content' in locals() else 'No content returned.'}")
        return pd.Series({
            "annotation": None,
            "explanation": "Parsing error or empty response"
        })



import spacy
nlp = spacy.load("en_core_web_sm")

def remove_named_entities(text):
    doc = nlp(text)
    tokens = [token.text for token in doc if not token.ent_type_ in {"PERSON", "ORG"}]
    return " ".join(tokens)



def main():
    df = pd.read_json("Data/final_dataset.json")
    df = df.dropna(subset=['description'])
    
    sample = df.sample(200, random_state=42)

    sample['clean_description'] = sample['description'].astype(str).apply(remove_named_entities)

    annotations_df = sample['clean_description'].apply(annotate_with_llm)
    sample = pd.concat([sample, annotations_df], axis=1)

    sample.to_csv("Data/llm_annotated_reviews_sample.csv", index=False)

if __name__ == "__main__":
    main()