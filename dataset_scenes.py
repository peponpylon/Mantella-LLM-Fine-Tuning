import pandas as pd
import numpy as np
import json

df_all = pd.read_csv('LazyVoiceFinder_export.csv', low_memory=False)
df_all = df_all.loc[~(df_all['State'].isin(['Bad File Name, No Dialogue','No Voice File']))]
df_all = df_all.loc[(df_all['Dialogue 1 - English'].notna()) & (df_all['Dialogue 1 - English']!='')]

filtered_cols = ['Topic Edid', 'Topic FormId', 'Edid', 'Response Num', 'Topic Text','Quest Edid','Quest Name','Category/Subtype','Emotion','Voice Type','Prompt', 'Conditions', 'Dialogue 1 - English']
df = df_all[filtered_cols]

# Extract scene dialogues
df_scene = df.loc[(df['Category/Subtype'] == 'Scene/Scene')]
print(len(df_scene))

def group_scene(df):
    grouped = df.groupby('Quest Edid')
    entries = []
    
    for questid, group in grouped:
        conversation = []

        # Remove duplicates within the group based on 'Dialogue 1 - English'
        group = group.drop_duplicates(subset=['Dialogue 1 - English'])

        # Aggregate data, concatenating dialogue and retaining other columns
        group_concatenated = group.groupby(['Response Num', 'Topic FormId']).agg({
            'Dialogue 1 - English': ' '.join,  # Concatenate the dialogue
            'Edid': 'first',                   # Keep the first value of another column
            'Voice Type': 'first',             # You can add more as needed
            'Quest Edid': 'first',
        }).reset_index()

        # Sort the aggregated group by 'Edid' for structured output
        sorted_group = group_concatenated.sort_values('Edid')

        # Create conversation entries
        for _, line in sorted_group.iterrows():
            conversation.append({"from": line['Voice Type'], "value": line['Dialogue 1 - English']})
            #print("Line: " + line['Voice Type'] + " Quest: " + line['Quest Edid'])

        entries.append({"conversations": conversation})

    return entries
        

data = group_scene(df_scene)
print(len(data))

for entry in data:
    for line in entry['conversations']:
        print(line)
        print("\n")
    print("\n=============================\n")

with open('skyrim_chatml_scenes_dataset.jsonl', 'w') as jsonl_file:
    for entry in data:
        json.dump(entry, jsonl_file)
        jsonl_file.write('\n')
