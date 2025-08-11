import ollama
import instructor
import openai
from pydantic import BaseModel
from typing import List, Optional

import re
import json

openai_api_key = []




class FashionDesigner(BaseModel):
    educated_at: Optional[List[list]] = None  # Optional, defaults to None
    employer: Optional[List[list]] = None    # Optional, defaults to None
    work_location: Optional[List[list]] = None  # Optional, defaults to None
    award_received: Optional[List[list]] = None  # Optional, defaults to None


def send_chat_prompt_instructor_fashion_designer(prompt, model, temperature = 0.5):
    client = instructor.from_openai(
        openai.OpenAI(
            base_url="http://localhost:11434/v1" if not "gpt" in model else None,
            api_key= "ollama" if not "gpt" in model else openai_api_key),
        mode=instructor.Mode.JSON,)
    resp = client.chat.completions.create(
        model=model,
        temperature = temperature,
        messages=[
                {"role": "system", "content": "You extract structured information from biographies about a fashion designer's education, employment, work locations, and awards. Only generate the output in JSON format. Use the example output as a reference for format only."},  
                {"role": "user", "content": prompt}],
        response_model= FashionDesigner,)
    response = (resp.model_dump_json(indent=2))
    return response

def main_generate_instructor_fashion_designer(prompt,model, temperature = 0.5):
    response = send_chat_prompt_instructor_fashion_designer(prompt,model, temperature)
    return response




class FashionHouse(BaseModel):
    founded_by: Optional[List[list]] = None 
    designer_employed: Optional[List[list]] = None  # Optional, defaults to None


def send_chat_prompt_instructor_fashion_house(prompt, model, temperature = 0.5):
    client = instructor.from_openai(
        openai.OpenAI(
            base_url="http://localhost:11434/v1" if not "gpt" in model else None,
            api_key= "ollama" if not "gpt" in model else openai_api_key),
        mode=instructor.Mode.JSON,)
    resp = client.chat.completions.create(
        model=model,
        temperature = temperature,
        messages=[
                {"role": "system", "content": "You extract structured information from text about fashion houses focusing on the designers. Only generate the output in JSON format. Use the example output as a reference for format only."},  
                {"role": "user", "content": prompt}],
        response_model= FashionHouse,)
    response = (resp.model_dump_json(indent=2))
    return response

def main_generate_instructor_fashion_house(prompt,model, temperature = 0.5):
    response = send_chat_prompt_instructor_fashion_house(prompt,model, temperature)
    return response


def find_names_in_text(names_list, text):
    # Escape the names in the list to avoid regex issues with special characters
    found_names = [name for name in names_list if re.search(rf'\b{re.escape(name)}\b', text)]
    
    # Sort names by length (longer first) to ensure longest names come first
    found_names = sorted(found_names, key=len, reverse=True)
    # Filter out shorter names that are substrings of longer names
    final_names = []
    for name in found_names:
        if not any(name in longer_name for longer_name in final_names):
            final_names.append(name)
    return final_names

def prompt_template(biography_example, biography_KG_example, biography_test, ontology = None, concepts = None, school = None, houses_designer = None):
    if ontology is None:
        basic_instruction = """Extract knowledge triples from the text.In the output, only include the triples in the given output format."""
    else:
        basic_instruction = f"""Given the following ontology and sentences, please extract the triples from the sentence according to the relations in the ontology. In the output, only include the triples in the given output format.
        {concepts}
        {ontology}"""
        
    if school is None:
        entities_from_text_example = None
        entities_from_text_test = None
    else:
        entities_from_text_example = f"""Entities of type academic institution in Example Sentence {find_names_in_text(school, biography_example)}
        \nEntities of type fashion house in Example Sentence {find_names_in_text(houses_designer, biography_example)}"""
        entities_from_text_test = f"""Entities of type academic institution in Test Sentence {find_names_in_text(school, biography_test)}
        \nEntities of type fashion house in Test Sentence {find_names_in_text(houses_designer, biography_test)}"""

    prompt_ont =f"""
    {basic_instruction}
    \nExample Sentence: {biography_example}
    {entities_from_text_example}
    \nExample Output:  {biography_KG_example}
    \nTest Sentence: {biography_test}
    {entities_from_text_test}
    \nTest Output:
    """
    return prompt_ont

def generate_prompt(bio, reference_text, reference_KG, ontology, concepts):
    return prompt_template(reference_text, reference_KG, bio, ontology, concepts)

def generate_kg( bio, reference_text, reference_KG,ontology, concepts,model, temperature=0.5):
    prompt = generate_prompt(bio, reference_text, reference_KG, ontology, concepts)
    response = main_generate_instructor_fashion_house(prompt,model, temperature)
    return json.loads(response)

def contains_synthetic_data(kg, synthetic_kg):
    synthetic_names = {entry[0] for entries in synthetic_kg.values() for entry in entries if entry}
    return any(name in json.dumps(kg) for name in synthetic_names)

def contains_none(kg):
    # Check if all values in the KG are None
    return not all(value is None for value in kg.values())

def is_valid_kg(kg, synthetic_KG):
    return contains_none(kg) and not contains_synthetic_data(kg, synthetic_KG)

