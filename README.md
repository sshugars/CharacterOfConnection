Replication materials for _The Character of Connection: Platform Affordances and Connective Democracy_. Sarah Shugars and Eunbin Ha, Social Media + Society, (2025). <https://doi.org/10.1177/20563051251332427>

Anonymized data for this study can be found at <https://sarahshugars.com/connectiveDemocracy_data.zip>

Code for this paper are organized into subfolders based on the steps in the analysis pipeline. See each folder for more details on that particular element.

**0_seed_posts**
Scripts for original (seed) post collection based on keyword search for both Twitter and Reddit.

**1_conversation_retrieval**
Scripts for sampling seed posts and retrieving full conversations for both Twitter and Reddit, as well as scripts for initial pre-processing of those posts. 

**2_computationalAnalysis**
Scripts for calculating toxicity (requires API), topic model, and general statistics about conversation structure.

**3_classification**
Classification of conversations into relevant topical areas.

**4_analysis**
Final analysis, creation of figures, and calculation of summary statistics.