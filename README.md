Otomoto Project – Step-by-Step Summary

Project Overview:
I developed an end-to-end, automated web scraping solution for Otomoto, a leading automotive marketplace. The solution extracts and processes vehicle listings data daily using an Azure Function, stores it in an Azure SQL database, and updates a Power BI dashboard for real-time analytics.

Step-by-Step Process:

Requirements Analysis and Planning:
- Defined project goals: automatically scrape DS 7 Crossback vehicle listings, extract key details (auction URL, vehicle specifications, price, location, etc.), and store the data in a SQL database.
- Determined challenges such as handling HTML parsing, secure database connectivity, and ensuring seamless daily updates.

Data Extraction and Processing:
- Developed a web scraper using the requests library with custom headers to fetch Otomoto's search pages.
- Utilized BeautifulSoup for HTML parsing, extracting critical listing details.
- Implemented data normalization functions and fuzzy matching algorithms to accurately detect specific car versions.
- Scheduled the scraper to run automatically daily using Azure Functions timer trigger.

Database Design and Integration:
- Designed a SQL database schema on an Azure SQL Server with tables for storing vehicle listings.
- Built Python functions to compute unique keys using MD5 hashing, generate sequential auction numbers, and insert records into the database using pymssql.
- Implemented SQL authentication using environment variables for secure credential management.

CI/CD and Cloud Deployment:
- Set up GitHub Actions workflow for continuous deployment to Azure Functions.
- Configured the necessary dependencies in requirements.txt and package management in the deployment workflow.
- Used environment variables in Azure Functions to store database credentials securely.

Automation and Integration with Power BI:
- Utilized Azure Functions timer trigger to run the scraper at 10:00 AM daily.
- Integrated the database with Power BI Service, enabling a dashboard that updates as new data is ingested.
- This automated pipeline allows stakeholders to access up-to-date insights without manual intervention.

Testing and Debugging:
- Implemented comprehensive logging throughout the application to monitor execution.
- Added error handling to ensure reliability and to help with debugging.
- Conducted thorough testing of each component to ensure the data pipeline works end-to-end.

Lessons Learned and Achievements:
- Gained expertise in web scraping, HTML parsing, and data normalization.
- Developed strong skills in cloud function deployment and database integration.
- Learned to manage and automate daily data pipelines using Azure Functions.
- Demonstrated the ability to integrate multiple technologies into a cohesive, automated solution.

Conclusion:
This project demonstrates my technical abilities in building an automated data pipeline combining web scraping, cloud functions, database integration, and business intelligence with Power BI. It highlights my proficiency with Python, Azure cloud services, and database technologies—key skills for a data analyst role.
