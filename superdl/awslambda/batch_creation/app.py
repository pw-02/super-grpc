import base64
import json
import zlib
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from io import BytesIO
import boto3
import redis
import torch
import torchvision
from PIL import Image
from queue import Queue, Empty
import torchvision.transforms as transforms
import unicodedata
import nltk
from nltk.corpus import stopwords, wordnet
from nltk.tokenize import word_tokenize
from transformers import GPT2Tokenizer
import re
import random
from collections import Counter
import os

# Externalize configuration parameters
#REDIS_HOST = '172.17.0.2'
#REDIS_HOST = 'host.docker.internal' #use this when testing locally on .dev container
#REDIS_PORT = 6379

# Configuration parameters
# REDIS_HOST = "localhost"
# REDIS_PORT = 6379
nltk.data.path.append(os.getenv('NLTK_DATA', '/var/task/nltk_data'))
s3_client = None
# redis_client = None
# Set the TRANSFORMERS_CACHE environment variable
os.environ['HF_HOME'] = '/tmp'
# Ensure the cache directory exists
# os.makedirs(os.environ['TRANSFORMERS_CACHE'], exist_ok=True)
tokenizer = None       
# tokenizer=GPT2Tokenizer.from_pretrained('gpt2')
# Check and download NLTK data if not already available
def check_and_download_nltk_resource(resource_name: str):
    try:
        nltk.data.find(resource_name)
    except LookupError:
        pass

# Regex patterns
UNICODE_PUNCT = {
    "，": ",", "。": ".", "、": ",", "„": '"', "”": '"', "“": '"', "«": '"', "»": '"',
    "１": '"', "」": '"', "「": '"', "《": '"', "》": '"', "´": "'", "∶": ":", "：": ":",
    "？": "?", "！": "!", "（": "(", "）": ")", "；": ";", "–": "-", "—": " - ", "．": ". ",
    "～": "~", "’": "'", "…": "...", "━": "-", "〈": "<", "〉": ">", "【": "[", "】": "]",
    "％": "%", "►": "-"
}
UNICODE_PUNCT_RE = re.compile(f"[{''.join(UNICODE_PUNCT.keys())}]")
NON_PRINTING_CHARS_RE = re.compile(f"[{''.join(map(chr, list(range(0,32)) + list(range(127,160))))}]")
DIGIT_RE = re.compile(r"\d")
PUNCT_OR_NON_PRINTING_CHARS_RE = re.compile((UNICODE_PUNCT_RE.pattern + NON_PRINTING_CHARS_RE.pattern).replace("][", ""))
class TextTransformations:
    """Class for text normalization and augmentation transformations."""

    def normalize(self, line: str, accent: bool = True, case: bool = True, numbers: bool = True, punct: int = 1) -> str:
        """Normalize a line of text."""
        line = line.strip()
        if not line:
            return line
        if case:
            line = line.lower()
        if accent:
            line = self.strip_accents(line)
        if numbers:
            line = DIGIT_RE.sub("0", line)
        if punct == 1:
            line = self.replace_unicode_punct(line)
        line = self.remove_non_printing_char(line)
        line = self.remove_pii(line)
        line = self.normalize_spacing_for_tok(line)
        line = self.remove_stop_words(line)
        line = self.remove_rare_words(line)
        line = self.random_insertion(line)
        line = self.word_swapping(line)
        line = self.synonym_replacement(line)
        return line

    def remove_stop_words(self, text: str) -> str:
        """Remove stop words from text."""
        tokens = text.split()
        stopwords_set = set(stopwords.words('english'))
        filtered_tokens = [word for word in tokens if word not in stopwords_set]
        return ' '.join(filtered_tokens)

    def remove_rare_words(self, text: str) -> str:
        """Remove rare words from text."""
        tokens = text.split()
        word_counts = Counter(tokens)
        threshold = 2
        filtered_tokens = [word for word in tokens if word_counts[word] > threshold]
        return ' '.join(filtered_tokens)

    def replace_with_synonym(self, word: str) -> str:
        """Replace a word with its synonym."""
        synonyms = [lemma.name() for syn in wordnet.synsets(word) for lemma in syn.lemmas()]
        return random.choice(synonyms) if synonyms else word

    def synonym_replacement(self, text: str, num_replacements: int = 1) -> str:
        """Replace words in text with their synonyms."""
        words = text.split()
        for _ in range(num_replacements):
            idx = random.randint(0, len(words) - 1)
            words[idx] = self.replace_with_synonym(words[idx])
        return ' '.join(words)

    def remove_pii(self, text: str) -> str:
        """Remove personally identifiable information (PII) from text."""
        pii_patterns = [
            (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]'),
            (r'\b(?:\+\d{1,2}\s*)?(?:\(\d{3}\)|\d{3})[-.\s]?\d{3}[-.\s]?\d{4}\b', '[PHONE]'),
            (r'\b\d{3}-\d{2}-\d{4}\b', '[SSN]'),
            (r'\b(?:\d[ -]*?){13,16}\b', '[CREDIT_CARD]'),
            (r'\b\d{1,5}\s+\w+\s+\w+\b', '[ADDRESS]')
        ]
        for pattern, replacement in pii_patterns:
            text = re.sub(pattern, replacement, text)
        return text

    def random_insertion(self, text: str, num_insertions: int = 1) -> str:
        """Insert random words into the text."""
        words = text.split()
        for _ in range(num_insertions):
            words.insert(random.randint(0, len(words)), random.choice(words))
        return ' '.join(words)

    def word_swapping(self, text: str, num_swaps: int = 1) -> str:
        """Swap random words in the text."""
        words = text.split()
        for _ in range(num_swaps):
            idx1, idx2 = random.sample(range(len(words)), 2)
            words[idx1], words[idx2] = words[idx2], words[idx1]
        return ' '.join(words)

    def remove_non_printing_char(self, text: str) -> str:
        """Remove non-printing characters from text."""
        return NON_PRINTING_CHARS_RE.sub("", text)

    def remove_html_tags(self, text: str) -> str:
        """Remove HTML tags from text."""
        clean = re.compile('<.*?>')
        return re.sub(clean, '', text)

    def remove_unicode_punct(self, text: str) -> str:
        """Remove Unicode punctuation from text."""
        return UNICODE_PUNCT_RE.sub("", text)

    def replace_unicode_punct(self, text: str) -> str:
        """Replace Unicode punctuation in text."""
        return "".join(UNICODE_PUNCT.get(c, c) for c in text)

    def strip_accents(self, line: str) -> str:
        """Strip accents from characters in the text."""
        nfd = unicodedata.normalize("NFD", line)
        return "".join(c for c in nfd if unicodedata.category(c) != "Mn")

    def normalize_spacing_for_tok(self, text: str, language: str = "en") -> str:
        """Normalize spacing for tokenization."""
        res = (
            text.replace("\r", "")
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
            .replace(" « ", ' "')
            .replace("« ", '"')
            .replace("«", '"')
            .replace(" » ", '" ')
            .replace(" »", '"')
            .replace("»", '"')
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
        if language == "en":
            res = re.sub(r"\"([,\.]+)", r"\1\"", res)
        else:
            res = res.replace(',"', '",')
            res = re.sub(r"(\.+)\"(\s*[^<])", r"\"\1\2", res)
        if language in {"de", "es", "cz", "cs", "fr"}:
            res = re.sub(r"(\d) (\d)", r"\1,\2", res)
        else:
            res = re.sub(r"(\d) (\d)", r"\1.\2", res)
        return res

def dict_to_torchvision_transform(transform_dict):
    """
    Converts a dictionary of transformations to a PyTorch transform object.
    """
    transform_list = []
    for transform_name, params in transform_dict.items():
        if transform_name == 'Resize':
            transform_list.append(torchvision.transforms.Resize(params))
        elif transform_name == 'Normalize':
            transform_list.append(torchvision.transforms.Normalize(mean=params['mean'], std=params['std']))
        elif params is None:
            transform_list.append(getattr(torchvision.transforms, transform_name)())
        else:
            raise ValueError(f"Unsupported transform: {transform_name}")

    return torchvision.transforms.Compose(transform_list)

def is_image_file(path: str):
    return any(path.endswith(extension) for extension in ['.jpg', '.JPG', '.jpeg', '.JPEG', '.png', '.PNG', '.ppm', '.PPM', '.bmp', '.BMP'])



def transform(bucket_name):


    if 'imagenet1k-sdl' in bucket_name:
        normalize = transforms.Normalize(
            mean=[0.485, 0.456, 0.406], 
            std=[0.229, 0.224, 0.225],
        )
        return transforms.Compose([
            transforms.Resize(256),                    # Resize the image to 256x256 pixels
            transforms.RandomResizedCrop(224),   # Randomly crop a 224x224 patch
            transforms.RandomHorizontalFlip(), # Randomly flip the image horizontally
            transforms.ToTensor(),  # Convert the image to a PyTorch tensor
            normalize,
        ])
    elif 'sdl-cifar10' in bucket_name:
         return transforms.Compose([
             transforms.ToTensor(),
             transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
             ])
    

def get_data_sample(bucket_name, data_sample,transformations):

    sample_path, sample_label = data_sample
    obj = s3_client.get_object(Bucket=bucket_name, Key=sample_path)

    if is_image_file(sample_path):
        content = obj['Body'].read()
        content = Image.open(BytesIO(content))
        content = content.convert("RGB") 
    else:
        content = obj['Body'].read().decode('utf-8')

    if transformations:
        return transformations(content), sample_label
    else:
        return transform(bucket_name)(content), sample_label
        # return torchvision.transforms.ToTensor()(content), sample_label

def create_text_tokens(bucket_name, samples):
    global tokenizer
    check_and_download_nltk_resource('stopwords')
    check_and_download_nltk_resource('wordnet')
    sample_path, sample_label = samples[0]
    obj = s3_client.get_object(Bucket=bucket_name, Key=sample_path)
    text = obj['Body'].read().decode('utf-8')
    text_transformer = TextTransformations()
    text = text_transformer.normalize(text)
    if tokenizer is None:
        # tokenizer = GPT2Tokenizer.from_pretrained('gpt2')
        tokenizer = GPT2Tokenizer.from_pretrained('/var/task/gpt2-tokenizer')

    tokens = tokenizer(text, truncation=False, padding=False, return_tensors='pt').input_ids.squeeze()
    buffer = BytesIO()
    torch.save(tokens, buffer)
    token_data = zlib.compress(buffer.getvalue()) #use_compression:
    token_data = base64.b64encode(token_data).decode('utf-8')
    return token_data


def create_minibatch(bucket_name, samples, transformations):
    sample_data, sample_labels = [], []
    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(get_data_sample, bucket_name, sample, transformations): sample for sample in samples}
        for future in concurrent.futures.as_completed(futures):
            file_path = futures[future]
            try:
                processed_tensor, label = future.result()
                sample_data.append(processed_tensor)
                sample_labels.append(label)
            except Exception as e:
                print(f"Error processing file {file_path}: {e}")

    minibatch = torch.stack(sample_data), torch.tensor(sample_labels)
    
    #Serialize the PyTorch tensor
    buffer = BytesIO()
    torch.save(minibatch, buffer)
    # serialized_mini_batch = buffer.getvalue()
    minibatch = zlib.compress(buffer.getvalue()) #use_compression:

    # Encode the serialized tensor with base64
    minibatch = base64.b64encode(minibatch).decode('utf-8')

    return minibatch

# Define a timeout handler function
def timeout_handler(signum, frame):
    raise TimeoutError("Redis set operation timed out")

def lambda_handler(event, context):
    """
    AWS Lambda handler function that processes a batch of images from an S3 bucket and caches the results in Redis.
    """
    try:
        task = event['task']
        if task == 'warmup':
            return {'success': True, 'message': 'function warmed'}
         
        bucket_name = event['bucket_name']
        batch_samples = event['batch_samples']
        batch_id = event['batch_id'] 
        cache_address = event['cache_address']
        # transformations = event['transformations']
        transformations =  None
        cache_host, cache_port = cache_address.split(":")

        global s3_client #, redis_client

        if s3_client is None:
            s3_client = boto3.client('s3')

        # if redis_client is None:
        redis_client = redis.StrictRedis(host=cache_host, port=cache_port) # Instantiate Redis client
           
        if task == 'vision':
             #deserailize transfor,ations
            if transformations:
                transformations = dict_to_torchvision_transform(json.loads(transformations))
            torch_minibatch = create_minibatch(bucket_name, batch_samples, transformations)

        elif task == 'language':
            torch_minibatch = create_text_tokens(bucket_name, batch_samples)

        # Cache minibatch in Redis using batch_id as the key
        redis_client.set(batch_id, torch_minibatch)
        return {
            'success': True,
            'batch_id': batch_id,
            'is_cached': True,
            'message': f"Successfully cached '{batch_id}'"
            }
    except Exception as e:
       return {
            'success': False,
            'batch_id': batch_id,
            'is_cached': False,
            'message': f"{str(e)}"
        }
