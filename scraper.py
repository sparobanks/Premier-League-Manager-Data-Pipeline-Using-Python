import argparse
import logging
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://www.transfermarkt.com"
PREMIER_LEAGUE_MANAGERS_URL = (
    "https://www.transfermarkt.com/premier-league/trainer/pokalwettbewerb/GB1"
)
DEFAULT_OUTPUT_DIR = Path("output")
DEFAULT_DELAY_MIN = 1.5
DEFAULT_DELAY_MAX = 3.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}

PROFILE_COLUMNS = [
    "manager_name",
    "full_name",
    "date_of_birth",
    "age",
    "place_of_birth",
    "citizenship",
    "contract_until",
    "average_term_as_coach",
    "coaching_licence",
    "preferred_formation",
    "agent",
    "transfermarkt_manager_id",
    "profile_url",
    "scraped_at",
]

CAREER_COLUMNS = [
    "manager_name",
    "transfermarkt_manager_id",
    "club",
    "role",
    "appointed_date",
    "in_charge_until",
    "days_in_charge",
    "matches",
    "wins",
    "draws",
    "losses",
    "players_used",
    "avg_goals",
    "ppm",
    "games_ppg",
    "assistant_manager_of",
    "profile_url",
    "scraped_at",
]


@dataclass
class ManagerLink:
    manager_name: str
    profile_url: str
    transfermarkt_manager_id: str


class TransfermarktManagerScraper:
    def __init__(self, delay_min: float = DEFAULT_DELAY_MIN, delay_max: float = DEFAULT_DELAY_MAX):
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.session = self._build_session()
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def _build_session() -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=3,
            read=3,
            connect=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.headers.update(HEADERS)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _sleep(self) -> None:
        duration = random.uniform(self.delay_min, self.delay_max)
        self.logger.info("Sleeping for %.2f seconds", duration)
        time.sleep(duration)

    def get_soup(self, url: str) -> BeautifulSoup:
        self.logger.info("Fetching %s", url)
        response = self.session.get(url, timeout=30)
        response.raise_for_status()
        self._sleep()
        return BeautifulSoup(response.text, "html.parser")

    @staticmethod
    def extract_manager_id(url: str) -> str:
        match = re.search(r"/trainer/(\d+)", url)
        return match.group(1) if match else ""

    @staticmethod
    def absolute_url(href: str) -> str:
        return urljoin(BASE_URL, href)

    @staticmethod
    def clean_text(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        cleaned = re.sub(r"\s+", " ", value.replace("\xa0", " ")).strip()
        return cleaned or None

    @staticmethod
    def normalize_date(value: Optional[str]) -> Optional[str]:
        value = TransfermarktManagerScraper.clean_text(value)
        if not value:
            return None
        value = value.replace("expected", "").strip()
        match = re.search(r"(\d{2}/\d{2}/\d{4})", value)
        return match.group(1) if match else value

    @staticmethod
    def text_after_label(text: str, label: str, stop_labels: Optional[List[str]] = None) -> Optional[str]:
        stop_labels = stop_labels or []
        pattern = rf"{re.escape(label)}\s*(.+?)"
        if stop_labels:
            stops = "|".join(re.escape(x) for x in stop_labels)
            pattern += rf"(?=\s*(?:{stops})\s*)"
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return None
        return TransfermarktManagerScraper.clean_text(match.group(1))

    @staticmethod
    def parse_age_from_dob(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        match = re.search(r"\((\d+)\)", value)
        return match.group(1) if match else None

    @staticmethod
    def strip_age_from_dob(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        value = re.sub(r"\s*\(\d+\)", "", value)
        return TransfermarktManagerScraper.clean_text(value)

    def get_current_premier_league_manager_links(self) -> List[ManagerLink]:
        soup = self.get_soup(PREMIER_LEAGUE_MANAGERS_URL)
        manager_links: List[ManagerLink] = []
        seen_ids = set()

        for anchor in soup.select('a[href*="/profil/trainer/"]'):
            href = anchor.get("href", "")
            manager_id = self.extract_manager_id(href)
            manager_name = self.clean_text(anchor.get_text(" ", strip=True))
            if not href or not manager_id or not manager_name:
                continue
            if manager_id in seen_ids:
                continue
            seen_ids.add(manager_id)
            manager_links.append(
                ManagerLink(
                    manager_name=manager_name,
                    profile_url=self.absolute_url(href.split("?")[0]),
                    transfermarkt_manager_id=manager_id,
                )
            )

        if len(manager_links) < 20:
            raise RuntimeError(
                f"Expected at least 20 manager profile links, found {len(manager_links)}."
            )

        return manager_links[:20]

    def parse_profile_page(self, manager: ManagerLink) -> Dict[str, Optional[str]]:
        soup = self.get_soup(manager.profile_url)
        page_text = soup.get_text("\n", strip=True)
        scraped_at = pd.Timestamp.utcnow().isoformat()

        manager_name = self.clean_text(
            (soup.select_one("h1") or soup.select_one("header h1")).get_text(" ", strip=True)
            if (soup.select_one("h1") or soup.select_one("header h1"))
            else manager.manager_name
        )

        dob_raw = self._extract_detail_value(soup, page_text, [
            "Date of birth/Age",
            "Date of birth / Age",
        ])
        full_name = self._extract_detail_value(soup, page_text, [
            "Full Name",
            "Name in Home Country / Full Name",
        ])
        place_of_birth = self._extract_detail_value(soup, page_text, ["Place of Birth", "Place of birth"])
        citizenship = self._extract_detail_value(soup, page_text, ["Citizenship"])
        contract_until = self._extract_detail_value(soup, page_text, ["Contract until"])
        average_term = self._extract_detail_value(soup, page_text, ["Avg. term as coach"])
        coaching_licence = self._extract_detail_value(soup, page_text, ["Coaching Licence", "Coaching Licence "])
        preferred_formation = self._extract_detail_value(soup, page_text, ["Preferred formation"])
        agent = self._extract_detail_value(soup, page_text, ["Agent"])

        profile_row = {
            "manager_name": manager_name,
            "full_name": full_name,
            "date_of_birth": self.strip_age_from_dob(dob_raw),
            "age": self.parse_age_from_dob(dob_raw),
            "place_of_birth": self.clean_text(place_of_birth),
            "citizenship": self.clean_text(citizenship),
            "contract_until": self.normalize_date(contract_until),
            "average_term_as_coach": self.clean_text(average_term),
            "coaching_licence": self.clean_text(coaching_licence),
            "preferred_formation": self.clean_text(preferred_formation),
            "agent": self.clean_text(agent),
            "transfermarkt_manager_id": manager.transfermarkt_manager_id,
            "profile_url": manager.profile_url,
            "scraped_at": scraped_at,
        }
        return profile_row

    def _extract_detail_value(
        self,
        soup: BeautifulSoup,
        page_text: str,
        labels: List[str],
    ) -> Optional[str]:
        # First try label-based HTML extraction
        for label in labels:
            for element in soup.find_all(string=re.compile(rf"^{re.escape(label)}\s*:?$", re.I)):
                parent = element.parent
                if not parent:
                    continue
                # sibling-based
                sibling_texts = []
                for sib in parent.next_siblings:
                    text = self.clean_text(getattr(sib, "get_text", lambda *a, **k: str(sib))(" ", strip=True) if hasattr(sib, "get_text") else str(sib))
                    if text:
                        sibling_texts.append(text)
                if sibling_texts:
                    return self.clean_text(" ".join(sibling_texts))
                parent_text = self.clean_text(parent.get_text(" ", strip=True))
                if parent_text and label.lower() in parent_text.lower():
                    value = re.sub(rf"^{re.escape(label)}\s*:?\s*", "", parent_text, flags=re.I)
                    value = self.clean_text(value)
                    if value and value.lower() != label.lower():
                        return value

        # Then try regex against the flattened page text
        stop_labels = [
            "Date of birth/Age",
            "Date of birth / Age",
            "Place of Birth",
            "Place of birth",
            "Citizenship",
            "Contract until",
            "Avg. term as coach",
            "Coaching Licence",
            "Preferred formation",
            "Agent",
            "Stats",
            "History",
            "Further details",
        ]
        for label in labels:
            regex = rf"{re.escape(label)}\s*:?\s*(.+?)(?=(?:{'|'.join(map(re.escape, stop_labels))})\s*:|$)"
            match = re.search(regex, page_text, flags=re.I | re.S)
            if match:
                value = self.clean_text(match.group(1))
                if value:
                    return value
        return None

    def build_detailed_history_url(self, profile_url: str) -> str:
        return re.sub(r"(/profil/trainer/\d+)(?:$|/)", r"\1/plus/1", profile_url)

    def parse_career_history(self, manager: ManagerLink) -> List[Dict[str, Optional[str]]]:
        detailed_url = self.build_detailed_history_url(manager.profile_url)
        soup = self.get_soup(detailed_url)
        scraped_at = pd.Timestamp.utcnow().isoformat()
        manager_name = self.clean_text((soup.select_one("h1") or soup.select_one("header h1")).get_text(" ", strip=True)) or manager.manager_name

        table = self._find_history_table(soup)
        if table is None:
            self.logger.warning("History table not found for %s", manager.profile_url)
            return []

        rows: List[Dict[str, Optional[str]]] = []
        pending_row_index: Optional[int] = None

        tbody = table.find("tbody") or table
        for tr in tbody.find_all("tr", recursive=False):
            row_text = self.clean_text(tr.get_text(" ", strip=True)) or ""
            if not row_text:
                continue

            # assistant-manager detail row often appears right after the main row
            if "Assistant Manager of:" in row_text and pending_row_index is not None:
                rows[pending_row_index]["assistant_manager_of"] = self.clean_text(
                    row_text.replace("Assistant Manager of:", "", 1)
                )
                continue

            tds = tr.find_all("td", recursive=False)
            if len(tds) < 6:
                continue

            club = None
            club_anchor = tr.select_one('a[href*="/startseite/verein/"]')
            if club_anchor:
                club = self.clean_text(club_anchor.get_text(" ", strip=True))
            if not club:
                club = self.clean_text(tds[0].get_text(" ", strip=True))
            if not club:
                continue

            role_text = self.clean_text(tds[1].get_text(" ", strip=True))
            appointed = self.normalize_date(tds[2].get_text(" ", strip=True)) if len(tds) > 2 else None
            in_charge_until = self.normalize_date(tds[3].get_text(" ", strip=True)) if len(tds) > 3 else None
            days_in_charge = self.clean_text(tds[4].get_text(" ", strip=True)) if len(tds) > 4 else None
            matches = self._clean_numeric_text(tds[5].get_text(" ", strip=True)) if len(tds) > 5 else None
            wins = self._clean_numeric_text(tds[6].get_text(" ", strip=True)) if len(tds) > 6 else None
            draws = self._clean_numeric_text(tds[7].get_text(" ", strip=True)) if len(tds) > 7 else None
            losses = self._clean_numeric_text(tds[8].get_text(" ", strip=True)) if len(tds) > 8 else None
            players_used = self._clean_numeric_text(tds[9].get_text(" ", strip=True)) if len(tds) > 9 else None
            avg_goals = self.clean_text(tds[10].get_text(" ", strip=True)) if len(tds) > 10 else None
            ppm = self.clean_text(tds[11].get_text(" ", strip=True)) if len(tds) > 11 else None
            games_ppg = self.clean_text(tds[12].get_text(" ", strip=True)) if len(tds) > 12 else None

            row = {
                "manager_name": manager_name,
                "transfermarkt_manager_id": manager.transfermarkt_manager_id,
                "club": club,
                "role": role_text,
                "appointed_date": appointed,
                "in_charge_until": in_charge_until,
                "days_in_charge": days_in_charge,
                "matches": matches,
                "wins": wins,
                "draws": draws,
                "losses": losses,
                "players_used": players_used,
                "avg_goals": avg_goals,
                "ppm": ppm,
                "games_ppg": games_ppg,
                "assistant_manager_of": None,
                "profile_url": manager.profile_url,
                "scraped_at": scraped_at,
            }
            rows.append(row)
            pending_row_index = len(rows) - 1

        return rows

    @staticmethod
    def _clean_numeric_text(value: Optional[str]) -> Optional[str]:
        value = TransfermarktManagerScraper.clean_text(value)
        if not value or value == "-":
            return None
        return value

    @staticmethod
    def _find_history_table(soup: BeautifulSoup):
        for table in soup.find_all("table"):
            table_text = table.get_text(" ", strip=True)
            if all(keyword in table_text for keyword in ["Club", "Appointed", "Matches"]):
                return table
        return None

    def scrape(self, output_dir: Path, export_excel: bool = True) -> Tuple[pd.DataFrame, pd.DataFrame]:
        output_dir.mkdir(parents=True, exist_ok=True)
        manager_links = self.get_current_premier_league_manager_links()
        self.logger.info("Found %d current Premier League managers", len(manager_links))

        profile_rows: List[Dict[str, Optional[str]]] = []
        career_rows: List[Dict[str, Optional[str]]] = []

        for index, manager in enumerate(manager_links, start=1):
            self.logger.info("Processing %d/%d: %s", index, len(manager_links), manager.manager_name)
            try:
                profile_rows.append(self.parse_profile_page(manager))
            except Exception as exc:
                self.logger.exception("Failed to parse profile for %s: %s", manager.profile_url, exc)
                profile_rows.append({
                    "manager_name": manager.manager_name,
                    "full_name": None,
                    "date_of_birth": None,
                    "age": None,
                    "place_of_birth": None,
                    "citizenship": None,
                    "contract_until": None,
                    "average_term_as_coach": None,
                    "coaching_licence": None,
                    "preferred_formation": None,
                    "agent": None,
                    "transfermarkt_manager_id": manager.transfermarkt_manager_id,
                    "profile_url": manager.profile_url,
                    "scraped_at": pd.Timestamp.utcnow().isoformat(),
                })

            try:
                career_rows.extend(self.parse_career_history(manager))
            except Exception as exc:
                self.logger.exception("Failed to parse career history for %s: %s", manager.profile_url, exc)

        profiles_df = pd.DataFrame(profile_rows, columns=PROFILE_COLUMNS)
        career_df = pd.DataFrame(career_rows, columns=CAREER_COLUMNS)

        profiles_path = output_dir / "manager_profiles.csv"
        career_path = output_dir / "manager_career_history.csv"
        profiles_df.to_csv(profiles_path, index=False)
        career_df.to_csv(career_path, index=False)

        if export_excel:
            excel_path = output_dir / "transfermarkt_managers.xlsx"
            with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
                profiles_df.to_excel(writer, sheet_name="manager_profiles", index=False)
                career_df.to_excel(writer, sheet_name="manager_career_history", index=False)
            self.logger.info("Wrote %s", excel_path)

        self.logger.info("Wrote %s", profiles_path)
        self.logger.info("Wrote %s", career_path)
        return profiles_df, career_df


def configure_logging(verbose: bool = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape current Premier League manager profiles and career history from Transfermarkt."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to write CSV/XLSX outputs into.",
    )
    parser.add_argument(
        "--delay-min",
        type=float,
        default=DEFAULT_DELAY_MIN,
        help="Minimum delay between requests in seconds.",
    )
    parser.add_argument(
        "--delay-max",
        type=float,
        default=DEFAULT_DELAY_MAX,
        help="Maximum delay between requests in seconds.",
    )
    parser.add_argument(
        "--no-excel",
        action="store_true",
        help="Skip Excel export.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging(args.verbose)
    scraper = TransfermarktManagerScraper(
        delay_min=args.delay_min,
        delay_max=args.delay_max,
    )
    scraper.scrape(output_dir=args.output_dir, export_excel=not args.no_excel)


if __name__ == "__main__":
    main()
