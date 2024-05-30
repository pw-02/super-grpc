# import nltk
# nltk.download('stopwords', download_dir='./nltk_data')
# nltk.download('wordnet', download_dir='./nltk_data')

from transformers import GPT2Tokenizer, GPT2Model

# Download and cache the tokenizer and model
tokenizer = GPT2Tokenizer.from_pretrained('gpt2')

# Save the tokenizer
tokenizer.save_pretrained('./gpt2-tokenizer')