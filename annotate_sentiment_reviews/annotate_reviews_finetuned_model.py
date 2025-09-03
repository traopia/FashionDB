from transformers import AutoTokenizer, AutoModelForSequenceClassification
import pandas as pd
from datasets import Dataset
import torch
import numpy as np
from annotate_reviews_llm import remove_named_entities


output_path = "Data/all_reviews_annotated_finetuning.csv"
full_data_path = "Data/final_dataset.json"
device = torch.device("mps") if torch.backends.mps.is_available() else torch.device("cpu")


model_dir = "./sentiment_model"  # or the absolute path if running from elsewhere
tokenizer = AutoTokenizer.from_pretrained(model_dir)
model = AutoModelForSequenceClassification.from_pretrained(model_dir)
model.to(device)  # Ensure model is on MPS

# Predict on all descriptions
all_df = pd.read_json(full_data_path)
all_df['clean_description'] = all_df['description'].astype(str).apply(remove_named_entities)


all_texts = all_df['clean_description'].astype(str).tolist()
batch_size = 32
preds = []
logits_list = []
model.eval()
for i in range(0, len(all_texts), batch_size):
    batch = all_texts[i:i+batch_size]
    inputs = tokenizer(batch, padding=True, truncation=True, max_length=256, return_tensors="pt")
    inputs = {k: v.to(device) for k, v in inputs.items()}
    # If you have any float tensors, cast them to float32:
    for k, v in inputs.items():
        if torch.is_floating_point(v):
            inputs[k] = v.float()
    with torch.no_grad():
        outputs = model(**inputs)
        batch_logits = outputs.logits.cpu().numpy()
        batch_preds = np.argmax(batch_logits, axis=1)
        preds.extend(batch_preds)
        logits_list.extend(batch_logits.tolist())  # Store both logits for each sample

all_df['annotation'] = preds
all_df['logits'] = logits_list

# Compute probability for class 1 using softmax
all_df['probs_logits'] = all_df['logits'].apply(lambda x: float(np.exp(x[1]) / np.sum(np.exp(x))))

# Remove columns if they exist
for col in ['label', 'predicted_sentiment']:
    if col in all_df.columns:
        all_df = all_df.drop(columns=[col])

all_df.to_csv(output_path, index=False)
print(f"Saved predictions to {output_path}") 