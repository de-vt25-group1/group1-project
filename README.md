# Sentiment Analysis on Reddit using Spark NLP

## Project Overview
This project analyzes sentiment in Reddit discussions at the subreddit level using **Apache Spark NLP**. The goal is to classify posts as **positive** or **negative**, rank subreddits based on sentiment ratios, and derive insights into how different communities engage in discussions.

## Dataset
We use the **Webis-TLDR-17 Corpus**, a large-scale dataset containing nearly 4 million Reddit posts. It includes structured metadata like subreddit names and post summaries, making it suitable for NLP applications such as **sentiment analysis** and **abstractive summarization**.

## Technologies Used
- **Apache Spark** for distributed data processing
- **Spark NLP** for sentiment classification
- **Google Cloud Platform (GCP)** for deployment
- **Python (pyspark, pandas, matplotlib)** for data manipulation and visualization

## Project Structure
```
project-repo/
├── notebooks/           # Jupyter notebooks with code and analysis
│   ├── code_100k-comments-checkpoint.ipynb   # Data cleaning and preparation steps
│   ├──code_100k-comments.ipynb  # Sentiment analysis initial code
│   ├── code_full.ipynb  # complete code of developed project
├── reports/             # Project Report
├── requirements.txt       # Required dependencies
├── README.md              # Project documentation
```

## Installation & Setup
To run this project locally, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-repo-url.git
   cd project-repo
   ```
2. **Set up your virtual machine and configure Apache Spark**:
   - Follow **Lab 3 of the Data Engineering course documentation** to deploy a virtual machine with Apache Spark.
   - Ensure that Spark and Hadoop are properly installed on your instance.
   - Configure `spark-env.sh` and `core-site.xml` as per the lab guidelines.
   - Verify the installation by running:
     ```bash
     spark-shell
     ```
   ```
3. **Install dependencies**:
   ```bash
   use pip to install required dependencies
   ```
4. **Run Jupyter Notebook**:
   ```bash
   jupyter notebook
   ```

## Usage Guide
- Open the `notebooks/` directory and start with `code_100kcomments.ipynb` to clean and prepare the dataset.
- Run `code_full.ipynb` to analyse posts as positive or negative using **Spark NLP**.

## Results & Insights
- The **top 10 most positive and negative subreddits** were identified.
- Discussions on societal issues tend to have more negative sentiment.
- Challenges faced: **Deploying Spark NLP on GCP** and handling multilingual content.

## Contribution Guidelines
Want to contribute? Follow these steps:
1. Fork the repository
2. Create a new branch (`git checkout -b feature-branch`)
3. Commit changes (`git commit -m "Added feature XYZ"`)
4. Push to the branch (`git push origin feature-branch`)
5. Create a Pull Request

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments
- **Uppsala University** for guidance and project framework.
- **Anthony Melinder, Diogo Miranda, Linjia Zhong, Petter Möllerström and Zhuoer Zhou** for collaborative effort in data engineering and analysis.
