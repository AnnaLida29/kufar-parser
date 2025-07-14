import requests
import re
import time
import data_client


class Parser:

    BASE_URL = "https://api.kufar.by/search-api/v2/search/rendered-paginated"
    PARAMS = {
        "cat": "1020",
        "cur": "USD",
        "gtsy": "country-belarus",
        "lang": "ru",
        "size": 30,
        "typ": "sell",
    }

    MAX_RECORDS = 300  # Максимум записей для парсинга

    @staticmethod
    def parse_elem_text(text):
        # Универсальное регулярное выражение для разбора цены, цены в $ и описания
        pattern = re.compile(
            r"(?P<price>\d[\d\s]*)\s*р\.\s*(?P<price_usd>\d[\d\s]*)?\s*\$?\s*(?P<description>.*)",
            re.DOTALL
        )
        match = pattern.match(text.strip())
        if match:
            price = match.group('price').replace(' ', '') if match.group('price') else ''
            price_usd = match.group('price_usd').replace(' ', '') if match.group('price_usd') else ''
            description = match.group('description').strip()
            return price, price_usd, description
        else:
            return '', '', text.strip()

    def get_ads_from_api(self, cursor=None):
        params = self.PARAMS.copy()
        if cursor:
            params["cursor"] = cursor

        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()

        ads = data.get("ads", [])
        pagination = data.get("pagination", {})
        pages = pagination.get("pages", [])

        next_cursor = None
        for p in pages:
            if p.get("label") == "next":
                next_cursor = p.get("token")
                break

        return ads, next_cursor

    def extract_items(self, ads):
        items = []
        for ad in ads:
            link = ad.get("ad_link")
            price = ad.get("price_byn")
            price_usd = ad.get("price_usd")
            description = ad.get("body_short") or ""

            # Приводим цены к целочисленному виду (цены в API в копейках)
            price_int = int(price) if price else 0
            price_usd_int = int(price_usd) if price_usd else 0

            items.append((link, price_int, price_usd_int, description))
        return items

    def save_to_db(self, items):
        connection = self.data_client_imp.get_connection()
        self.data_client_imp.create_table(connection)
        for item in items:
            self.data_client_imp.insert(connection, item[0], item[1], item[2], item[3])

    def run(self):
        self.data_client_imp = data_client.CsvClient()  # Или другой клиент
        total = 0
        next_cursor = None
        all_items = []

        while True:
            if total >= self.MAX_RECORDS:
                print("Достигнут лимит записей.")
                break

            ads, next_cursor = self.get_ads_from_api(next_cursor)
            if not ads:
                print("Парсинг завершён — страниц больше нет.")
                break

            items = self.extract_items(ads)
            all_items.extend(items)

            total += len(items)
            print(f"Обработано: {total}")

            time.sleep(1)  # Чтобы не перегружать сервер

            if not next_cursor:
                print("Парсинг завершён — страниц больше нет.")
                break

        self.save_to_db(all_items)
        print(f" Всего загружено: {len(all_items)}")


if __name__ == "__main__":
    Parser().run()
