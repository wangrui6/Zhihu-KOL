### Install requirements
`pip install -r requirements.txt`

### Install Playwright [headless browser](https://playwright.dev/python/docs/intro)
`playwright install` 

Run the following commands to install necessary library if needed
`sudo apt-get install libatk1.0-0 libatk-bridge2.0-0 libcups2 libatspi2.0-0 libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libgbm1 libxkbcommon0 libpango-1.0-0 libcairo2 libasound2`

### Download data based on single KOL url token
`python main.py`

### Download data based on roundtable topics
`python scrape_by_topic.py`

### Convert to Parquet
`python convert_parquet.py`

### Upload to HF
`python upload_hf.py`



      