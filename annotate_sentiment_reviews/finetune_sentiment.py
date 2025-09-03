import pandas as pd
from datasets import Dataset
from transformers import AutoTokenizer, AutoModelForSequenceClassification, TrainingArguments, Trainer
from sklearn.model_selection import train_test_split
import torch
import numpy as np
from sklearn.metrics import accuracy_score, f1_score
import os
from annotate_reviews_llm import remove_named_entities


annotated_path = "Data/llm_annotated_reviews_sample.csv"



# Load annotated data
sample_df = pd.read_csv(annotated_path)
sample_df = sample_df.dropna(subset=['description'])
sample_df['clean_description'] = sample_df['description'].astype(str).apply(remove_named_entities)
sample_df['label'] = sample_df['annotation']

# Split into train/validation

from sklearn.utils import resample

# Separate majority and minority classes
df_majority = sample_df[sample_df['label'] == 1]
df_minority = sample_df[sample_df['label'] == 0]

# Upsample minority class
df_minority_upsampled = resample(
    df_minority,
    replace=True,  # sample with replacement
    n_samples=len(df_majority),  # to match majority class
    random_state=42
)

# Combine majority class with upsampled minority class
sample_df_balanced = pd.concat([df_majority, df_minority_upsampled])

# Shuffle the dataset
sample_df_balanced = sample_df_balanced.sample(frac=1, random_state=42).reset_index(drop=True)
train_df, val_df = train_test_split(sample_df_balanced, test_size=0.2, random_state=42, stratify=sample_df_balanced['label'])

# Compute class weights for imbalance
class_counts = np.bincount(train_df['label'])
class_weights = 1. / class_counts
class_weights = class_weights / class_weights.sum() * len(class_counts)
class_weights = torch.tensor(class_weights, dtype=torch.float)

# Convert to HuggingFace Dataset
train_dataset = Dataset.from_pandas(train_df[['clean_description', 'label']])
val_dataset = Dataset.from_pandas(val_df[['clean_description', 'label']])

# Tokenize
tokenizer = AutoTokenizer.from_pretrained("distilbert-base-uncased")
def tokenize(batch):
    return tokenizer([str(x) for x in batch['clean_description']], padding="max_length", truncation=True, max_length=256)
train_dataset = train_dataset.map(tokenize, batched=True)
val_dataset = val_dataset.map(tokenize, batched=True)
train_dataset.set_format(type='torch', columns=['input_ids', 'attention_mask', 'label'])
val_dataset.set_format(type='torch', columns=['input_ids', 'attention_mask', 'label'])

# Load model
model = AutoModelForSequenceClassification.from_pretrained("distilbert-base-uncased", num_labels=2)
device = torch.device("mps") if torch.backends.mps.is_available() else torch.device("cpu")
model.to(device)
class_weights = class_weights.to(device)

# Custom Trainer with weighted loss
from transformers import Trainer
class WeightedTrainer(Trainer):
    def compute_loss(self, model, inputs, return_outputs=False, **kwargs):
        labels = inputs.get("labels")
        outputs = model(**inputs)
        logits = outputs.get("logits")
        loss_fct = torch.nn.CrossEntropyLoss(weight=class_weights)
        loss = loss_fct(logits, labels)
        return (loss, outputs) if return_outputs else loss

def compute_metrics(eval_pred):
    logits, labels = eval_pred
    preds = np.argmax(logits, axis=1)
    return {
        "accuracy": accuracy_score(labels, preds),
        "f1": f1_score(labels, preds, average="weighted")
    }

training_args = TrainingArguments(
    output_dir="./sentiment_model",
    eval_strategy="epoch",
    save_strategy="epoch",
    learning_rate=2e-5,
    per_device_train_batch_size=8,
    per_device_eval_batch_size=8,
    num_train_epochs=10,
    weight_decay=0.01,
    load_best_model_at_end=True,
    metric_for_best_model="accuracy"
)

trainer = WeightedTrainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    compute_metrics=compute_metrics,
)

trainer.train()
eval_results = trainer.evaluate()
print("Validation results:", eval_results)

trainer.save_model("./sentiment_model")
tokenizer.save_pretrained("./sentiment_model")

