import pytest
import os
from playwright.async_api import async_playwright
from modules.oddsportal.scraper_data import OddsPortalDataMixin

@pytest.mark.asyncio
async def test_betfair_extraction_from_html():
    html_path = r"c:\Users\gadie\Documents\projects\sofascore\logs\debug\oddsportal\debug_los-angeles-dodgers-nwPDBpVc\op_fail_20260811_195123_215718_betfair_missing_data_full_time.html"
    
    assert os.path.exists(html_path), f"HTML debug file not found at {html_path}"
    
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        
        # Load the HTML content directly into the headless browser page
        await page.set_content(html_content)
        
        # Extract data using the scraper mixin method
        result = await OddsPortalDataMixin()._extract_data(page, "https://www.oddsportal.com/baseball/h2h/kansas-city-royals-IL2QbgJ4/los-angeles-dodgers-nwPDBpVc/#xGCazJ4k")
        
        # Assertions to verify correct Betfair Exchange odds parsing
        assert result.betfair is not None, "Betfair Exchange odds were not extracted"
        assert result.betfair.back_1 == "1.39", f"Expected Back 1 to be 1.39, got {result.betfair.back_1}"
        assert result.betfair.back_2 == "3.4", f"Expected Back 2 to be 3.4, got {result.betfair.back_2}"
        assert result.betfair.back_x == "-", f"Expected Back X to be '-', got {result.betfair.back_x}"
        
        assert result.betfair.lay_1 == "1.41", f"Expected Lay 1 to be 1.41, got {result.betfair.lay_1}"
        assert result.betfair.lay_2 == "3.55", f"Expected Lay 2 to be 3.55, got {result.betfair.lay_2}"
        assert result.betfair.lay_x == "-", f"Expected Lay X to be '-', got {result.betfair.lay_x}"
        
        await browser.close()
