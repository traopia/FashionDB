from transformers import AutoTokenizer, AutoModelForSequenceClassification
import os
import pandas as pd
from datasets import Dataset
import torch
import numpy as np
from annotate_reviews_llm import remove_named_entities


output_path = "data/data_vogue_final_reviews.parquet"
full_data_path = "data/data_vogue_final.parquet"
device = torch.device("mps") if torch.backends.mps.is_available() else torch.device("cpu")


script_dir = os.path.dirname(os.path.abspath(__file__))
model_root_dir = os.path.join(script_dir, "sentiment_model")
# Load tokenizer from the base model used in training
tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
# Load the fine-tuned model weights from the latest checkpoint
model_checkpoint_dir = os.path.join(model_root_dir, "checkpoint-75")
model = AutoModelForSequenceClassification.from_pretrained(model_checkpoint_dir, local_files_only=True)
model.to(device)  # Ensure model is on MPS

# Predict on all descriptions
all_df = pd.read_parquet(full_data_path)
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

all_df.to_parquet(output_path)
all_df = all_df.drop_duplicates(subset=["URL"])
print(f"Saved predictions to {output_path}") 