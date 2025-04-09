import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import random
import os
import logging
from urllib.parse import quote_plus
from datetime import datetime

# Configure logger
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_filename = os.path.join(
    log_dir, f'scraping_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Add console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(
    '%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console_handler)


def read_counties(file_path):
    """Read counties and states from the filtered_counties.txt file."""
    counties = []
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            parts = line.strip().rsplit(',', 1)
            if len(parts) == 2:
                county, state = parts
                counties.append((county.strip(), state.strip()))
            else:
                logger.warning(f"Skipping invalid line: {line.strip()}")
    return counties


def load_processed_counties(checkpoint_file):
    """Load the list of already processed counties from the checkpoint file."""
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r', encoding='utf-8') as file:
            return set(line.strip() for line in file)
    return set()


def save_processed_county(checkpoint_file, county, state):
    """Save a processed county to the checkpoint file."""
    with open(checkpoint_file, 'a', encoding='utf-8') as file:
        file.write(f"{county}, {state}\n")


async def safe_goto(page, url, attempt=1, max_attempts=3):
    """Avoid detection by adding delays & random mouse movements with retry logic."""
    if attempt > max_attempts:
        raise Exception(
            f"Failed to navigate to {url} after {max_attempts} attempts")

    await asyncio.sleep(random.uniform(3, 7))  # Random delay
    await page.mouse.move(random.randint(0, 1000), random.randint(0, 800))
    logger.info(f"Navigating to: {url} (Attempt {attempt}/{max_attempts})")

    try:
        await page.goto(url, timeout=120000)  # 2 min timeout
        # Perform a random scroll to mimic human behavior
        await page.mouse.wheel(0, random.randint(100, 300))
    except Exception as e:
        logger.error(f"Navigation error: {e}")
        await asyncio.sleep(random.uniform(10, 15))  # Longer delay between retries
        return await safe_goto(page, url, attempt + 1, max_attempts)


async def scrape_population_growth(page, county, state):
    """Scrape population growth rate from Bing."""
    search_query = f"{county}, {state} population growth rate"
    url = f"https://www.bing.com/search?q={quote_plus(search_query)}"
    try:
        await safe_goto(page, url)
        logger.info(f"Searching Bing for population growth rate: {search_query}")

        # Wait for the search results to load
        await page.wait_for_selector("#b_results", timeout=120000)

        # Extract all text from the page
        page_content = await page.inner_text("#b_results")
        logger.info(f"Page content loaded for {county}, {state}")

        # Find the first occurrence of a word containing '%'
        growth_rate = "N/A"
        for word in page_content.split():
            if "%" in word:
                growth_rate = word
                break

        logger.info(f"Population growth rate result: {growth_rate}")
        return growth_rate
    except Exception as e:
        logger.error(f"Error scraping population growth rate for {county}, {state}: {e}")
        return "N/A"


async def scrape_demographics(page, county, state):
    """Scrape demographics and population growth rate."""
    url = f"https://data.census.gov/all?q={quote_plus(f'{county}, {state}')}"
    try:
        await safe_goto(page, url)
        logger.info(f"Scraping demographics for {county}, {state}...")

        # Wait for the page to load
        await page.wait_for_selector(".highlight-description", timeout=120000)

        # Extract population
        population_element = await page.query_selector('div.highlight-description:has-text("Total Population:")')
        population = await population_element.inner_text() if population_element else "N/A"
        logger.info(f"Population: {population}")

        # Extract income
        income_element = await page.query_selector('div.highlight-description:has-text("Median Household Income:")')
        income = await income_element.inner_text() if income_element else "N/A"
        logger.info(f"Income: {income}")

        # Extract employment rate
        employment_element = await page.query_selector('div.highlight-description:has-text("Employment Rate:")')
        employment_rate = await employment_element.inner_text() if employment_element else "N/A"
        logger.info(f"Employment Rate: {employment_rate}")

        # Scrape population growth rate from Bing
        growth_rate = await scrape_population_growth(page, county, state)

        return {
            "County": county,
            "State": state,
            "Population": population.replace("Total Population:", "").strip() if population_element else "N/A",
            "Income": income.replace("Median Household Income:", "").strip() if income_element else "N/A",
            "Employment Rate": employment_rate.replace("Employment Rate:", "").strip() if employment_element else "N/A",
            "Population Growth Rate": growth_rate
        }
    except Exception as e:
        logger.error(f"Error scraping demographics for {county}, {state}: {e}")
        return {
            "County": county,
            "State": state,
            "Population": "N/A",
            "Income": "N/A",
            "Employment Rate": "N/A",
            "Population Growth Rate": "N/A"
        }


async def main():
    logger.info("Starting the main function")
    async with async_playwright() as p:
        logger.info("Launching the browser")
        # Launch the browser
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )

        page = await context.new_page()
        logger.info("Browser launched and new page created")

        # Set a longer default timeout
        page.set_default_timeout(60000)

        # Create output directory if it doesn't exist
        script_dir = os.path.dirname(__file__)
        output_dir = os.path.join(script_dir, "demographics_data")
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory created at {output_dir}")

        # Use a fixed CSV file name
        csv_path = os.path.join(output_dir, "data.csv")
        logger.info(f"CSV file path set to {csv_path}")

        # Initialize the CSV file with headers if it doesn't exist
        if not os.path.exists(csv_path):
            df = pd.DataFrame(columns=["County", "State", "Population", "Income", "Employment Rate", "Population Growth Rate"])
            df.to_csv(csv_path, index=False)
            logger.info("CSV file initialized with headers")

        # Read counties from filtered_counties.txt
        counties_file = os.path.join(os.path.dirname(__file__), "filtered_counties.txt")
        filtered_counties = read_counties(counties_file)
        logger.info(f"Loaded {len(filtered_counties)} counties from file")

        # Load already processed counties
        checkpoint_file = os.path.join(output_dir, "processed_counties.txt")
        processed_counties = load_processed_counties(checkpoint_file)
        logger.info(f"Loaded {len(processed_counties)} already processed counties")

        for county, state in filtered_counties:
            if f"{county}, {state}" in processed_counties:
                logger.info(f"Skipping already processed county: {county}, {state}")
                continue

            logger.info(f"Processing county: {county}, state: {state}")

            try:
                # Get county demographics and population growth rate
                demographics = await scrape_demographics(page, county, state)

                # Append the data to the single CSV file
                df = pd.DataFrame([demographics])
                df.to_csv(csv_path, mode='a', header=False, index=False)

                # Save the processed county to the checkpoint file
                save_processed_county(checkpoint_file, county, state)

                # Add an explicit break to separate counties in the console
                logger.info("-" * 80)

            except Exception as e:
                logger.error(f"Error processing {county}, {state}: {e}")
                # Continue to the next county despite errors
                continue

        await browser.close()
        logger.info("Browser closed")


if __name__ == "__main__":
    logger.info("Script started")
    try:
        asyncio.run(main())
    except asyncio.TimeoutError:
        logger.error("Script timed out")
    logger.info("Script finished")