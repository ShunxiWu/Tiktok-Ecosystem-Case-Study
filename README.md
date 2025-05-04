# TikTok Governance Issues Analysis

This project analyzes TikTok governance issues by scraping Twitter for relevant comments, classifying them using AI, and presenting the results in an interactive dashboard.

## Features

- **Data Collection**: Fetches Twitter comments about TikTok governance issues using the Twitter API
- **AI Classification**: Uses OpenAI's GPT models to classify issues as unhandled, mishandled, or non-issues
- **Data Storage**: Stores all collected and classified data in MongoDB
- **Interactive Dashboard**: Visualizes the data using Streamlit with interactive filtering and analysis tools
- **Automated Scheduling**: Uses APScheduler to regularly update the dataset with new content

## Project Structure

- `app.py`: Main Streamlit dashboard application
- `fetchdata.py`: Twitter data collection script
- `picking.py`: AI-powered classification script
- `config.py`: Configuration and environment settings
- `schedule.py`: Automated scheduling of data collection and processing

## Setup Instructions

### Prerequisites

- Python 3.9+
- MongoDB database
- RapidAPI account (for Twitter API access)
- OpenAI API account

### Environment Variables

Create a `.env` file with the following variables:

```
MONGO_URI=your_mongodb_connection_string
RAPIDAPI_KEY=your_rapidapi_key
OPENAI_API_KEY=your_openai_api_key
```

### Installation

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the dashboard: `streamlit run app.py`

### Deployment

This project is configured for deployment on Render with the included `render.yaml` file.

## Dashboard

The dashboard provides:
- Overall distribution of issue types
- Categorical breakdown of issues
- Time series analysis of different issue types
- Detailed data tables with filtering options

## License

[Add your chosen license here]

## Acknowledgements

- Twitter API (via RapidAPI)
- OpenAI GPT models
- Streamlit
- MongoDB
