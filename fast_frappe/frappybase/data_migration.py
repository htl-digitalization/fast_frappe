import pandas as pd
from openpyxl import load_workbook

def excel_metadata(path):
    # Read the Excel file
    df = pd.read_excel(path, engine='openpyxl')

    # Initialize an empty list to store metadata information
    metadata = []

    # Iterate over each column, extracting metadata
    for column in df.columns:
        header_name = column
        unique_values = df[column].dropna().unique()
        suggested_dtype = pd.api.types.infer_dtype(df[column])

        # Append the metadata to the list
        metadata.append({
            'Header Name': header_name,
            'Unique Values': unique_values,
            'Suggested Data Type': suggested_dtype
        })

    # Convert the metadata list into a Pandas DataFrame and return it
    metadata_df = pd.DataFrame(metadata)
    return metadata_df


def gpt_suggested_datatype(metadata_df):
    # Initialize an empty list to store the GPT suggested data types
    gpt_data_types = []

    # Iterate over each row in the metadata DataFrame
    for index, row in metadata_df.iterrows():
        header_name = row['Header Name']
        unique_values = row['Unique Values']

        # Prepare a prompt for GPT
        prompt = f"Please suggest a data type for the column '{header_name}' which has the following unique values: {unique_values}"

        # Make a call to the GPT model using the openai package
        response = openai.Completion.create(
            engine="text-davinci-002",
            prompt=prompt,
            max_tokens=10,
            n=1,
            stop=None,
            temperature=0.5,
        )

        # Extract the GPT suggestion from the response
        gpt_suggestion = response.choices[0].text.strip()

        # Append the GPT suggested data type to the list
        gpt_data_types.append(gpt_suggestion)

    # Add a new column to the metadata DataFrame with the GPT suggested data types
    metadata_df['GPT Suggested Data Type'] = gpt_data_types

    return metadata_df


metadata_df = excel_metadata("path_to_your_excel_file.xlsx")
metadata_with_gpt_suggestions = gpt_suggested_datatype(metadata_df)
print(metadata_with_gpt_suggestions)
