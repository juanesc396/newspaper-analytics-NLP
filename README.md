# NLP-in-newspaper-headlines
 This project seeks to train an nlp algorithm based on headlines scraped from newspapers.

In recent times, I have been observing how there is a large amount of news with negative content. For this reason I set out to learn Natural Language Processing while finding out what percentage of news was negative. This project seeks to answer that unknown.

The tecnologies i used are:

- Scrapy: Scrape the newspapers
- Spacy: Library that allows me to train a Text Classification model
- Pandas: ETL 
- MongoDB: To store the data from newspapers

The model was tested by F1-Score. 

The Steps I followed were:
1. Create Spiders (Scrapy classes that work as a scrapers).
2. Recolect data from several newspapers with spiders.
3. Clean the data extracted and stored in MongoDB.
4. Create the model with Spacy.
5. Test the model to measure it's efficiency.
6. Analize the dataset scraped with the trained model with the goal to determinate how much news are negative.
