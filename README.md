Here is my ER diagram for the main data I'll be organizing for the business:
![ER Diagram](docs/Cookie_ERD.png)

## Generate data
I don't like paying for a ChatGPT subscription, and you can't export CSV files in the free version, so I had it generate pre-formatted dummy data I could easily copy, paste, and plug into a Python script that loaded it all into CSVs that were immediately usable for the project. I know in real life the source data might not be quite this neat and ready to use, but there are still some imperfections that I'll be able to clean up in the transform stage.

## Write the CSVs
The writecsvs.py file takes all the raw data above and uses pandas to write it all to four separate CSVs and save it to the appropriate location. In a real-world scenario, I'd most likely use the platform's API to automate data refreshes for orders data (with an automated python script to clean the data as necessary). Recipe, ingredient, and product updates might need to be done manually in this kind of small business scenario where those things don't change super regularly.

## Extract
