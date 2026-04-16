# TableauDoc

Generate comprehensive documentation for your Tableau users to avoid spending weeks writing what you already know. No Tableau knowledge required to create or understand the output.

## Instructions
1. Open your preferred AI. I use Sonnet because I generally tend to have better results with it.
2. Upload the Tableau_Dashboard_Documentation.docx and tableau_parser.py files. Give the AI the prompt stored in sonnet_prompt.txt and modify as desired.
3. Upload your workbook as either a twb or xml file to the agent with any prompt (Eg. "Here is the workbook"). Note: some AI agents do not accept twb files, so uploading an xml is safer.
4. **If your AI agent does not allow python files, then you must set up a Python environment and run the python code yourself with your workbook in the same folder. You can then upload the resulting JSON file in place of your workbook.**
