import torch
import torchvision
import base64
import boto3
import redis
import zlib
import os
import re
import unicodedata
import nltk
import random
from io import BytesIO
from nltk.corpus import wordnet
nltk.download('wordnet')
nltk.download('stopwords')
import torch.nn.functional as F
import tiktoken
# Externalize configuration parameters
#REDIS_HOST = '172.17.0.2'
#REDIS_HOST = 'host.docker.internal' #use this when testing locally on .dev container
#REDIS_PORT = 6379

REDIS_HOST =  "10.0.31.114"
# REDIS_HOST =  "ec2-34-217-48-32.us-west-2.compute.amazonaws.com"

REDIS_PORT = 6378

s3_client = boto3.client('s3')
redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT) # Instantiate Redis client

use_local = False

UNICODE_PUNCT = {
    "，": ",",
    "。": ".",
    "、": ",",
    "„": '"',
    "”": '"',
    "“": '"',
    "«": '"',
    "»": '"',
    "１": '"',
    "」": '"',
    "「": '"',
    "《": '"',
    "》": '"',
    "´": "'",
    "∶": ":",
    "：": ":",
    "？": "?",
    "！": "!",
    "（": "(",
    "）": ")",
    "；": ";",
    "–": "-",
    "—": " - ",
    "．": ". ",
    "～": "~",
    "’": "'",
    "…": "...",
    "━": "-",
    "〈": "<",
    "〉": ">",
    "【": "[",
    "】": "]",
    "％": "%",
    "►": "-",
}
UNICODE_PUNCT_RE = re.compile(f"[{''.join(UNICODE_PUNCT.keys())}]")
# Build a regex matching all control characters.
NON_PRINTING_CHARS_RE = re.compile(
    f"[{''.join(map(chr, list(range(0,32)) + list(range(127,160))))}]"
)
DIGIT_RE = re.compile(r"\d")
PUNCT_OR_NON_PRINTING_CHARS_RE = re.compile(
    (UNICODE_PUNCT_RE.pattern + NON_PRINTING_CHARS_RE.pattern).replace("][", "")
)


def normalize(line: str, accent=True, case=True, numbers=True, punct=1) -> str:
        line = line.strip()
        if not line:
            return line
        if case:
            line = line.lower()
        if accent:
            line = strip_accents(line)
        if numbers:
            line = DIGIT_RE.sub("0", line)
        if punct == 1:
            line = replace_unicode_punct(line)
        line = remove_non_printing_char(line)
        
        line = remove_pii(line)
        line = normalize_spacing_for_tok(line)
        line = remove_stop_words(line)
        line = remove_rare_words(line)
        line = random_insertion(line)
        line = word_swapping(line)
        line = synonym_replacement(line)

        return line
    
def remove_stop_words(text):
        tokens = text.split()
        from nltk.corpus import stopwords
        stopwords_set = set(stopwords.words('english'))
        filtered_tokens  = [word for word in tokens if word not in stopwords_set]
        filtered_text = ' '.join(filtered_tokens)
        return filtered_text

def remove_rare_words(text):
        from collections import Counter
        tokens = text.split()
        # Count the frequency of each token
        word_counts = Counter(tokens)
        # Set a threshold for word frequency
        threshold = 2
        # Remove tokens with frequency less than or equal to the threshold
        filtered_tokens = [word for word in tokens if word_counts[word] > threshold]
        filtered_text = ' '.join(filtered_tokens)
        return filtered_text
        # Function to perform synonym replacement augmentation
    
def replace_with_synonym(self,word):
        synonyms = []
        for syn in wordnet.synsets(word):
            for lemma in syn.lemmas():
                synonyms.append(lemma.name())
        return random.choice(synonyms) if synonyms else word
    
def synonym_replacement(self, text, num_replacements=1):
        words = text.split()
        for _ in range(num_replacements):
            idx = random.randint(0, len(words) - 1)
            words[idx] = replace_with_synonym(words[idx])
        return ' '.join(words)

    
def remove_pii(text):
        # Regular expressions for various PII types
        email_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        phone_regex = r'\b(?:\+\d{1,2}\s*)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}\b'
        ssn_regex = r'\b\d{3}-\d{2}-\d{4}\b'
        credit_card_regex = r'\b(?:\d[ -]*?){13,16}\b'
        address_regex = r'\b\d{1,5}\s+\w+\s+\w+\b'

        # Replace PII with placeholders
        text = re.sub(email_regex, '[EMAIL]', text)
        text = re.sub(phone_regex, '[PHONE]', text)
        text = re.sub(ssn_regex, '[SSN]', text)
        text = re.sub(credit_card_regex, '[CREDIT_CARD]', text)
        text = re.sub(address_regex, '[ADDRESS]', text)
        return text
    
# Function to perform random insertion augmentation
def random_insertion(self, text, num_insertions=1):
        words = text.split()
        for _ in range(num_insertions):
            words.insert(random.randint(0, len(words)), random.choice(words))
        return ' '.join(words)
    
# Function to perform word swapping augmentation
def word_swapping(self, text, num_swaps=1):
        words = text.split()
        for _ in range(num_swaps):
            idx1, idx2 = random.sample(range(len(words)), 2)
            words[idx1], words[idx2] = words[idx2], words[idx1]
        return ' '.join(words)


def remove_non_printing_char(self, text: str) -> str:
        return NON_PRINTING_CHARS_RE.sub("", text)


def normalize_spacing_for_tok(text: str, language: str = "en") -> str:
        res = (
            text.replace("\r", "")
            # remove extra spaces
            .replace("(", " (")
            .replace(")", ") ")
            .replace(" +", " ")
        )
        res = re.sub(r"\) ([\.\!\:\?\;\,])", r"\)\1", res)
        res = res.replace("( ", "(").replace(" )", ")")
        res = re.sub(r"(\d) \%", r"\1\%", res)
        res = res.replace(" :", ":").replace(" ;", ";")
        res = res.replace("`", "'").replace("''", ' " ')

        res = (
            res.replace("„", '"')
            .replace("“", '"')
            .replace("”", '"')
            .replace("–", "-")
            .replace("—", " - ")
            .replace(" +", " ")
            .replace("´", "'")
            .replace("([a-z])‘([a-z])", r"\1'\2/")
            .replace("([a-z])’([a-z])", r"\1'\2/")
            .replace("‘", '"')
            .replace("‚", '"')
            .replace("’", '"')
            .replace("''", '"')
            .replace("´´", '"')
            .replace("…", "...")
            # French quotes
            .replace(" « ", ' "')
            .replace("« ", '"')
            .replace("«", '"')
            .replace(" » ", '" ')
            .replace(" »", '"')
            .replace("»", '"')
            # handle pseudo-spaces
            .replace(" %", "%")
            .replace("nº ", "nº ")
            .replace(" :", ":")
            .replace(" ºC", " ºC")
            .replace(" cm", " cm")
            .replace(" ?", "?")
            .replace(" !", "!")
            .replace(" ;", ";")
            .replace(", ", ", ")
            .replace(" +", " ")
            .replace("．", ". ")
        )
        # English "quotation," followed by comma, style
        if language == "en":
            res = re.sub(r"\"([,\.]+)", r"\1\"", res)
        # Czech is confused
        elif language == "cs" or language == "cz":
            pass
        # German/Spanish/French "quotation", followed by comma, style
        else:
            res = res.replace(',"', '",')
            res = re.sub(
                r"(\.+)\"(\s*[^<])", r"\"\1\2", res
            )  # don't fix period at end of sentence

        if (
            language == "de"
            or language == "es"
            or language == "cz"
            or language == "cs"
            or language == "fr"
        ):
            res = re.sub(r"(\d) (\d)", r"\1,\2", res)
        else:
            res = re.sub(r"(\d) (\d)", r"\1.\2", res)
        return res


def remove_html_tags(text):
        import re
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)
    
def remove_unicode_punct(text: str) -> str:
        """More aggressive version of replace_unicode_punct but also faster."""
        return UNICODE_PUNCT_RE.sub("", text)
    
def replace_unicode_punct(text: str) -> str:
        return "".join((UNICODE_PUNCT.get(c, c) for c in text))
    
def strip_accents(line: str) -> str:
        """Strips accents from a piece of text."""
        nfd = unicodedata.normalize("NFD", line)
        output = [c for c in nfd if unicodedata.category(c) != "Mn"]
        if len(output) == line:
            return line
        return "".join(output)


def download_file(bucket_name, file_path):
# Print current working directory
    if use_local:
        os.chdir('/workspaces/super-dl/')
        file_path = os.path.join(os.getcwd(), file_path)
        with open(file_path, 'rb') as file:
            content = file.read()
        return content
    else:
        # Download file into memory
        obj = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        content = obj['Body'].read().decode('utf-8')
        return content


def tokenize(text):
        tokenizer=tiktoken.get_encoding("gpt2")
        ids = tokenizer.encode_ordinary(text) 
        
        #encode_ordinary ignores any special tokens
        #ids.append(tokenizer.eot_token) # add the end of text token, e.g. 50256 for gpt2 bpe
        # print(f"tokens: {len(ids)}")
        # Tokenize text into chunks of block_size
        block_size = 2048
        chunks = []
        start_idx = 0
        while start_idx < len(ids):
            end_idx = min(start_idx + block_size, len(ids))
            x = torch.tensor(ids[start_idx:end_idx], dtype=torch.long)
            y = torch.tensor(ids[start_idx+1:end_idx+1], dtype=torch.long)
            if len(x) < block_size:
                # print(len(ids) + (block_size - len(x)))
                x = F.pad(x, (0, block_size - len(x)))
            if len(y) < block_size:
                y = F.pad(y, (0, block_size - len(y)))
         
            chunks.append((x, y))
            start_idx = end_idx

        return chunks

def create_torch_batch(bucket_name, batch_metadata, transformations):
    file_path = batch_metadata[0]
    sample_label = batch_metadata[1]
    sample_input = download_file(bucket_name, file_path)
    normalized_input = normalize(sample_input)
    tokenized_chunks = tokenize(normalized_input)
    return torch.stack(tokenized_chunks)

def lambda_handler(event, context):
    try:
        #Extract information from the event
        # body = event['body']
        # event = json.loads(body)

        bucket_name = event['bucket_name']
        batch_metadata = event['batch_metadata']
        batch_id = event['batch_id']
        use_compression = True

        if bucket_name == 'foo':
            return {'statusCode': 200,'message': 'function_warmed'}

        #'batch_tensor' contains a batch of PyTorch tensors representing the images
        tensor_batch = create_torch_batch(bucket_name, batch_metadata)

        # Serialize the PyTorch tensor to binary data, then get the serialized data from the buffer
        buffer = BytesIO()
        torch.save(tensor_batch, buffer)
        serialized_tensor_batch = buffer.getvalue()

        if use_compression:
            serialized_tensor_batch = zlib.compress(serialized_tensor_batch)

        # Encode with base64
        serialized_tensor_batch = base64.b64encode(serialized_tensor_batch).decode('utf-8')

      # cache_batch in redis using batch_id
        is_cached = False
        try:
            redis_client.set(batch_id, serialized_tensor_batch)
            is_cached = True
            message = f''        
        except Exception as e:
            is_cached = False
            message = f"Failed to cache batch '{batch_id}' Error: {str(e)})"

        return {'statusCode': 200,
                'batch_id': batch_id,
                'is_cached': is_cached,
                'message': message}

    except Exception as e:
        return {
            'statusCode': 500,
            'batch_id': batch_id,
            'is_cached': False,
            'message': f'Error: {str(e)}'
        }