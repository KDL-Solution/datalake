user_prompt_dict = {
    "recognition": "Read every text.",
    "base_kie_bbox": "Extract information from the image. Return the result in the following structured JSON format (formatted with zero-space indentation and without newlines), filling in both <|value|> and <|bbox|>:",
    "base_kie_no_bbox": "Extract information from the image. Return the result in the following structured JSON format (formatted with zero-space indentation and without newlines), filling in <|value|>:",
    "post_handwritten_plain_text": "Extract information from the image. Return a structured JSON (compact, no spaces or newlines), filling in each <|value|>. Read from left to right, top to bottom:",
    "base_layout_reading_order": "Extract all layout elements. Reading order must be preserved.",
    "base_layout_no_reading_order": "Extract all layout elements. Reading order does not matter."
}
