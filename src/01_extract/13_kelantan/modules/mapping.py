import re, datetime
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

class Mapping:
    @staticmethod
    def get_current_datetime():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def extract_element_text(listing, xpath):
        try:
            element = listing.find_element(By.XPATH, xpath)
            return element.text.strip()
        except NoSuchElementException:
            return None

    @staticmethod   
    def extract_page_link(listing):
        try:
            page_link_element = listing.find_element(By.XPATH, ".//div[contains(@class,'slick-slide slick-active slick-current')]/a[contains(@class,'depth-listing-card-link')] | .//div[contains(@class,'BasicCardstyle__ListingImageWrapper-iZfXFa kjZOzk')]/a[contains(@class,'depth-listing-card-link')] | .//div[contains(@class,'ListingContactDetailsButtonstyle__ButtonsWrapper')]/button[contains(@class,'ListingContactDetailsButtonstyle__ButtonItem')]/a[contains(@class,'depth-listing-card-link')]")
            return page_link_element.get_attribute("href")
        except NoSuchElementException:
            return None

    @staticmethod
    def extract_source(listing):
        try:
            page_link = Mapping.extract_page_link(listing)
            if page_link is not None:
                page_link_split = page_link.split("/")
                page_source = page_link_split[2].split(".")[1]
                return page_source
            else:
                return None
        except NoSuchElementException:
            return None

    @staticmethod
    def split_psf_element(psf_element):
        return psf_element.split()[0].lstrip('(') + " " + psf_element.split()[1]

    @staticmethod
    def extract_price_square_feet(listing):
        try:
            if psf_element := Mapping.extract_element_text(
                listing,
                ".//div[contains(@class,'ListingPricestyle__PricePSFWrapper')]",
            ):
                return Mapping.split_psf_element(psf_element)
        except NoSuchElementException:
            return None

    @staticmethod
    def extract_house_type(listing):
        if house_type_text := Mapping.extract_element_text(
            listing,
            ".//p[contains(@class,'ListingAttributesstyle__ListingAttrsDescriptionItemWrapper')]",
        ):
            house_type_parts = re.split(r'\||•', house_type_text)
            return house_type_parts[0].strip()
        return None

    @staticmethod
    def extract_lot_type(listing):
        house_type_text = Mapping.extract_element_text(listing, ".//p[contains(@class,'ListingAttributesstyle__ListingAttrsDescriptionItemWrapper')]")
        if house_type_text and (lot_type_match := re.search(r'\|([^•]+)', house_type_text)):
            return lot_type_match[1].strip()
        return None

    @staticmethod
    def extract_square_footage(listing):
        house_type_text = Mapping.extract_element_text(listing, ".//p[contains(@class,'ListingAttributesstyle__ListingAttrsDescriptionItemWrapper')]")
        if house_type_text and (sq_footage_match := re.search(r'(?i)(?:Built-up|Land\s*area)\s*:\s*(.*?)\s*sq\. ft\.', house_type_text)):
            return sq_footage_match[1].replace(',', '')
        return None

    @staticmethod
    def extract_house_furniture(listing):
        house_type_text = Mapping.extract_element_text(listing, ".//p[contains(@class,'ListingAttributesstyle__ListingAttrsDescriptionItemWrapper')]")
        if house_type_text and 'furnished' in house_type_text.lower():
            return house_type_text.rsplit('•', 1)[-1].strip()

    @staticmethod
    def extract_text_content(listing, xpath):
        return Mapping.extract_element_text(listing, xpath)