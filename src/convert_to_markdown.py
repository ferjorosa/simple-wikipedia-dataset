import re
from os import getenv
from typing import Tuple

from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_openai import ChatOpenAI
from transformers import PreTrainedTokenizerFast

from src.utils import count_tokens


def convert_text_to_markdown(
    model_openrouter: str, raw_text: str, template: str
) -> str:
    """
    Converts raw text into clean, properly formatted markdown using a language
    model.

    Args:
        model (str): The model to use for transformation.
        raw_text (str): The input text to be formatted.

    Returns:
        str: The formatted markdown text.
    """
    # Load the prompt from the YAML file using the imported function
    # Create a prompt template
    prompt = PromptTemplate(template=template, input_variables=["text"])

    # Initialize the language model
    llm = ChatOpenAI(
        openai_api_key=getenv("OPENROUTER_API_KEY"),
        openai_api_base="https://openrouter.ai/api/v1",
        model_name=model_openrouter,
    )

    # Initialize the output parser
    output_parser = StrOutputParser()

    # Create a chain for processing the text
    chain = prompt | llm | output_parser

    # Invoke the chain to format the text
    return chain.invoke(input={"text": raw_text})


def convert_long_text_to_markdown(
    model_openrouter: str,
    raw_text: str,
    template: str,
    tokenizer: PreTrainedTokenizerFast,
    max_tokens: int,
) -> Tuple[str, list[str]]:
    """
    Processes the input text into Markdown by splitting it into sections,
    estimating the token count for each section, combining sections into
    batches that almost reach (but do not surpass) the specified token limit,
    and then formatting each batch.

    Args:
        model_openrouter (str): The model to use for Markdown transformation.
        raw_text (str): The input text to process.
        template (str): The template to use for formatting.
        tokenizer (PreTrainedTokenizerFast): The tokenizer to use for counting
                                             tokens.
        max_tokens (int): The maximum number of tokens per batch.

    Returns:
        str: The processed Markdown text.
        list[str]: A list of processed sections.
    """
    # Divide the text into sections
    sections = _divide_into_sections(raw_text)

    # Estimate the number of tokens for each section
    section_token_counts = [count_tokens(tokenizer, section) for section in sections]

    # Combine sections into batches that almost reach the max_tokens limit
    batches = []
    current_batch = []
    current_token_count = 0

    for section, token_count in zip(sections, section_token_counts):
        if current_token_count + token_count <= max_tokens:
            current_batch.append(section)
            current_token_count += token_count
        else:
            batches.append(current_batch)
            current_batch = [section]
            current_token_count = token_count

    # Add the last batch if it's not empty
    if current_batch:
        batches.append(current_batch)

    # Process each batch using the convert_text_to_markdown function
    processed_sections = []
    for batch in batches:
        combined_text = "\n\n".join(batch)
        processed_text = convert_text_to_markdown(
            model_openrouter, combined_text, template
        )
        processed_sections.append(processed_text)

    # Concatenate the processed sections back into a single text
    processed_text = "\n\n".join(processed_sections)

    return processed_text, processed_sections


def _divide_into_sections(text: str) -> list:
    """
    Divides the input text into sections based on titles marked with `=` signs.

    Args:
        text (str): The input text to be divided into sections.

    Returns:
        list: A list of sections, where each section includes the title and its
              related content.
    """
    # Regex to identify sections based on titles marked with `=` signs
    section_regex = r"(={1,5}\s*[^=]+\s*={1,5})"

    # Split the text into sections using the regex
    sections = re.split(section_regex, text)

    # Combine titles with their directly related content
    combined_sections = []
    for i in range(1, len(sections), 2):  # Start from 1 to skip the first empty element
        title = sections[i]
        content = sections[i + 1].strip()  # Remove leading/trailing whitespace
        combined_sections.append(f"{title}\n{content}")

    return combined_sections
