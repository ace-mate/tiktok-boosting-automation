#!/usr/bin/env python3
"""TikTok campaign boost automation.

Workflow implemented:
1) Build YYYY_MM_DD tag for today.
2) Find most recent campaign with name containing configured substring.
3) Duplicate that campaign.
4) Rename the duplicated campaign.
5) Fetch ad groups under the new campaign.
6) Clear bid on each ad group when bid_price is set.
7) Fetch ads under the new campaign.
8) Fetch the latest TikTok post item id.
9) Rename each ad and update creative tiktok_item_id.
10) Enable campaign, ad groups, and ads.
"""

from __future__ import annotations

import argparse
import copy
import datetime as dt
import html
import json
import logging
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_BASE_URL = "https://business-api.tiktok.com/open_api/v1.3"
DEFAULT_ADVERTISER_ID = "7441895227339718673"
DEFAULT_CAMPAIGN_SUBSTRING = "acemate_Views"
DEFAULT_POST_LIST_ENDPOINTS = "/spark_ad/post/list/,/tt_video/list/"
DEFAULT_SLACK_CHANNEL_ID = "C0A49B277UK"
DEFAULT_TIKTOK_MANAGE_CAMPAIGN_URL = (
    "https://ads.tiktok.com/i18n/manage/campaign?aadvid=7441895227339718673&lifetime=1"
)


class TikTokAPIError(RuntimeError):
    """Raised when the TikTok API returns an error response."""


class TikTokBusinessClient:
    def __init__(self, base_url: str, access_token: str, timeout_seconds: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token
        self.timeout_seconds = timeout_seconds

    def get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        return self._request("GET", path, params=params)

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        return self._request("POST", path, payload=payload)

    def _request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{path}"

        if params:
            query = urllib.parse.urlencode(params, doseq=True)
            url = f"{url}?{query}"

        body_bytes = None
        if payload is not None:
            body_bytes = json.dumps(payload).encode("utf-8")

        request = urllib.request.Request(
            url=url,
            data=body_bytes,
            method=method,
            headers={
                "Access-Token": self.access_token,
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
                raw_body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise TikTokAPIError(
                f"HTTP {exc.code} calling {path}: {error_body}"
            ) from exc
        except urllib.error.URLError as exc:
            raise TikTokAPIError(f"Network error calling {path}: {exc}") from exc

        try:
            parsed = json.loads(raw_body)
        except json.JSONDecodeError as exc:
            raise TikTokAPIError(f"Invalid JSON from {path}: {raw_body[:300]}") from exc

        if not isinstance(parsed, dict):
            raise TikTokAPIError(f"Unexpected response shape from {path}: {parsed!r}")

        code = parsed.get("code")
        if code not in (0, "0", None):
            message = parsed.get("message") or parsed.get("msg") or "Unknown API error"
            raise TikTokAPIError(f"TikTok API error on {path}: code={code}, message={message}")

        data = parsed.get("data")
        if data is None:
            return {}
        if isinstance(data, dict):
            return data
        return {"list": data}


@dataclass
class AutomationConfig:
    advertiser_id: str
    campaign_name_substring: str
    source_campaign_name_exact: str | None
    date_tag: str
    enable_entities: bool
    dry_run: bool
    post_list_endpoints: list[str]
    latest_item_id_override: str | None
    video_auth_code: str | None
    allow_ad_item_fallback: bool


class TikTokBoostAutomation:
    def __init__(self, client: TikTokBusinessClient, config: AutomationConfig) -> None:
        self.client = client
        self.config = config

    def run(self) -> dict[str, Any]:
        date_tag = self.config.date_tag
        logging.info("Step 1/10: date tag = %s", date_tag)
        desired_campaign_name = f"{self.config.campaign_name_substring}_{date_tag}"
        new_campaign_name = self._make_unique_campaign_name(desired_campaign_name)
        if new_campaign_name != desired_campaign_name:
            logging.info(
                "Campaign name %s already exists. Using unique name %s.",
                desired_campaign_name,
                new_campaign_name,
            )

        if self.config.source_campaign_name_exact:
            logging.info("Step 2/10: finding source campaign by exact name '%s'", self.config.source_campaign_name_exact)
            source_campaign = self._find_campaign_by_exact_name(self.config.source_campaign_name_exact)
        else:
            logging.info("Step 2/10: finding latest campaign with substring '%s'", self.config.campaign_name_substring)
            source_campaign = self._find_latest_campaign(self.config.campaign_name_substring)
        source_campaign_id = _pick_id(source_campaign, ["campaign_id", "id"])
        source_campaign_name = source_campaign.get("campaign_name") or source_campaign.get("name")
        logging.info("Using source campaign: id=%s name=%s", source_campaign_id, source_campaign_name)

        if self.config.latest_item_id_override:
            latest_item_id = _coerce_tiktok_item_id(str(self.config.latest_item_id_override))
            logging.info("Step 8/10: using manual TikTok item id override = %s", latest_item_id)
        else:
            logging.info("Step 8/10: fetching latest TikTok post for creative swap")
            if self.config.video_auth_code:
                logging.info("Step 8/10: authorizing TikTok video auth code before latest-post fetch")
                self._authorize_tt_video_auth_code(self.config.video_auth_code)
            try:
                latest_item_id = self._find_latest_post_item_id()
            except RuntimeError as exc:
                if not self.config.allow_ad_item_fallback:
                    raise RuntimeError(
                        f"{exc} To force a specific video use --latest-item-id, "
                        "or provide a fresh --video-auth-code to refresh authorized posts."
                    ) from exc
                fallback_item_id = self._find_latest_item_id_from_all_ads()
                if not fallback_item_id:
                    raise
                logging.warning(
                    "Post endpoint could not provide a usable latest post (%s). "
                    "Falling back to latest tiktok_item_id from existing ads: %s",
                    exc,
                    fallback_item_id,
                )
                latest_item_id = fallback_item_id
            logging.info("Latest TikTok post item id = %s", latest_item_id)

        if self.config.dry_run:
            logging.info("Dry-run enabled: stopping before any write operations.")
            return {
                "dry_run": True,
                "source_campaign_id": source_campaign_id,
                "source_campaign_name": source_campaign_name,
                "latest_item_id": latest_item_id,
                "planned_new_campaign_name": new_campaign_name,
            }

        logging.info("Step 3/10: duplicating campaign %s", source_campaign_id)
        new_campaign_id, new_campaign_name = self._copy_campaign(
            campaign_id=source_campaign_id,
            source_campaign=source_campaign,
            target_campaign_name=new_campaign_name,
            latest_item_id=latest_item_id,
        )
        logging.info("Created duplicated campaign id = %s", new_campaign_id)

        logging.info("Step 4/10: renaming campaign %s to %s", new_campaign_id, new_campaign_name)
        self._update_campaign(new_campaign_id, {"campaign_name": new_campaign_name})

        logging.info("Step 5/10: fetching ad groups for campaign %s", new_campaign_id)
        adgroups = self._get_entities_for_campaign("/adgroup/get/", new_campaign_id, "adgroups")
        logging.info("Found %d ad groups in duplicated campaign", len(adgroups))

        logging.info("Step 6/10: clearing bid_price for eligible ad groups")
        cleared_bid_adgroup_ids = self._clear_adgroup_bids(adgroups)

        logging.info("Step 7/10: fetching ads for campaign %s", new_campaign_id)
        ads = self._get_entities_for_campaign("/ad/get/", new_campaign_id, "ads")
        if not ads and adgroups:
            logging.warning(
                "Duplicated campaign %s has 0 ads after copy. Backfilling ads from source campaign %s.",
                new_campaign_id,
                source_campaign_id,
            )
            created_from_source = self._backfill_ads_from_source_campaign(
                source_campaign_id=source_campaign_id,
                target_adgroups=adgroups,
                tiktok_item_id=latest_item_id,
            )
            logging.info("Backfill created %d ads from source campaign.", created_from_source)
            ads = self._wait_for_ads_count(
                campaign_id=str(new_campaign_id),
                min_count=max(1, int(created_from_source)),
                max_attempts=6,
                sleep_seconds=3,
            )
        logging.info("Found %d ads in duplicated campaign", len(ads))
        if not ads:
            raise RuntimeError(
                "Duplicated campaign has no ads after copy/backfill. "
                "Aborting to avoid launching an empty campaign."
            )

        logging.info("Step 9/10: renaming ads and swapping creatives to latest post")
        updated_ad_ids = self._update_ads(ads, ad_name=date_tag, tiktok_item_id=latest_item_id)
        ad_alignment = self._ensure_ads_alignment(
            campaign_id=str(new_campaign_id),
            ad_name=date_tag,
            tiktok_item_id=latest_item_id,
        )
        ads = self._get_entities_for_campaign("/ad/get/", str(new_campaign_id), "ads")
        if ad_alignment.get("mismatched_ad_ids"):
            logging.warning(
                "Some ads are still mismatched after retries: %s",
                ad_alignment.get("mismatched_ad_ids"),
            )

        enabled = {"campaign": False, "adgroups": 0, "ads": 0}
        if self.config.enable_entities:
            logging.info("Step 10/10: enabling campaign, ad groups, and ads")
            self._set_campaign_status([new_campaign_id], "ENABLE")
            enabled["campaign"] = True
            enabled["adgroups"] = self._enable_adgroups(adgroups)
            enabled["ads"] = self._enable_ads(ads)
        else:
            logging.info("Step 10/10 skipped: --no-enable was provided")

        return {
            "dry_run": False,
            "source_campaign_id": source_campaign_id,
            "new_campaign_id": new_campaign_id,
            "new_campaign_name": new_campaign_name,
            "latest_item_id": latest_item_id,
            "adgroups_found": len(adgroups),
            "ads_found": len(ads),
            "adgroups_bid_cleared": len(cleared_bid_adgroup_ids),
            "ads_updated": len(updated_ad_ids),
            "ad_alignment": ad_alignment,
            "enabled": enabled,
        }

    def _find_latest_campaign(self, name_substring: str) -> dict[str, Any]:
        campaigns = self._list_entities("/campaign/get/")
        name_substring_lower = name_substring.lower()

        matches = []
        for campaign in campaigns:
            campaign_name = str(campaign.get("campaign_name") or campaign.get("name") or "")
            if name_substring_lower in campaign_name.lower():
                matches.append(campaign)

        if not matches:
            raise RuntimeError(
                f"No campaigns found containing '{name_substring}'. "
                f"Total campaigns fetched: {len(campaigns)}"
            )

        matches.sort(key=lambda item: _parse_time_value(item.get("create_time")), reverse=True)
        return matches[0]

    def _find_campaign_by_exact_name(self, campaign_name: str) -> dict[str, Any]:
        campaigns = self._list_entities("/campaign/get/")
        matches = [
            campaign
            for campaign in campaigns
            if str(campaign.get("campaign_name") or campaign.get("name") or "") == campaign_name
        ]
        if not matches:
            raise RuntimeError(f"No campaign found with exact name '{campaign_name}'.")
        matches.sort(key=lambda item: _parse_time_value(item.get("create_time")), reverse=True)
        return matches[0]

    def _authorize_tt_video_auth_code(self, auth_code: str) -> None:
        payload = {
            "advertiser_id": self.config.advertiser_id,
            "auth_code": str(auth_code),
        }
        self.client.post("/tt_video/authorize/", payload)

    def _find_latest_post_item_id(self) -> str:
        tried_endpoints: list[str] = []

        for endpoint in self.config.post_list_endpoints:
            endpoint = endpoint.strip()
            if not endpoint:
                continue
            tried_endpoints.append(endpoint)

            try:
                posts = self._list_entities(endpoint)
            except TikTokAPIError as exc:
                if "HTTP 404" in str(exc):
                    logging.debug("Post endpoint not found: %s", endpoint)
                    continue
                raise

            candidates: list[tuple[float, int, str]] = []
            has_real_timestamp = False
            expired_item_ids: list[str] = []
            for index, post in enumerate(posts):
                item_id = _extract_post_item_id(post)
                if not item_id:
                    continue

                ad_auth_status = _extract_ad_auth_status(post)
                if ad_auth_status == "EXPIRED":
                    expired_item_ids.append(item_id)
                    continue

                timestamp = _extract_post_time(post)
                if timestamp > 0:
                    has_real_timestamp = True
                candidates.append((timestamp, index, item_id))

            if candidates:
                logging.info("Using post endpoint %s", endpoint)
                if has_real_timestamp:
                    # Prefer explicit publish/create timestamps when available.
                    candidates.sort(key=lambda triple: (triple[0], -triple[1]), reverse=True)
                    return candidates[0][2]

                # Some endpoints return posts without publish timestamps and ignore sort params.
                # In that case, select the numerically largest item_id as newest fallback.
                candidates.sort(key=lambda triple: (_item_id_sort_key(triple[2]), -triple[1]), reverse=True)
                logging.info("No post timestamp fields found; using max item_id fallback for latest item selection.")
                return candidates[0][2]

            if expired_item_ids:
                sample = ", ".join(expired_item_ids[:5])
                raise RuntimeError(
                    f"Endpoint {endpoint} returned only EXPIRED ad-authorized posts (sample item_ids: {sample}). "
                    "Re-authorize TikTok posts for Spark Ads, then rerun."
                )

        attempted = ", ".join(tried_endpoints) if tried_endpoints else "(none configured)"
        raise RuntimeError(f"No TikTok posts found from configured endpoints: {attempted}")

    def _find_latest_item_id_from_all_ads(self) -> str | None:
        ads = self._list_entities("/ad/get/")
        item_ids: list[str] = []
        for ad in ads:
            value = ad.get("tiktok_item_id")
            if value is None:
                continue
            text = str(value).strip()
            if text:
                item_ids.append(text)
        if not item_ids:
            return None
        item_ids.sort(key=_item_id_sort_key, reverse=True)
        return item_ids[0]

    def _copy_campaign(
        self,
        campaign_id: str,
        source_campaign: dict[str, Any],
        target_campaign_name: str,
        latest_item_id: str,
    ) -> tuple[str, str]:
        payload = {
            "advertiser_id": self.config.advertiser_id,
            "campaign_ids": [str(campaign_id)],
        }
        try:
            data = self.client.post("/campaign/copy/", payload)
        except TikTokAPIError as exc:
            if "HTTP 404" not in str(exc):
                raise
            logging.warning(
                "/campaign/copy/ is unavailable for this account. Falling back to create-based duplication."
            )
            return self._manual_duplicate_campaign(
                source_campaign=source_campaign,
                target_campaign_name=target_campaign_name,
                latest_item_id=latest_item_id,
            )

        new_campaign_id = _pick_id(data, ["new_campaign_id", "campaign_id", "campaign_ids", "ids"])
        if not new_campaign_id:
            raise RuntimeError(f"Unable to find new campaign id from /campaign/copy/ response: {data}")
        return str(new_campaign_id), str(target_campaign_name)

    def _manual_duplicate_campaign(
        self,
        source_campaign: dict[str, Any],
        target_campaign_name: str,
        latest_item_id: str,
    ) -> tuple[str, str]:
        source_campaign_id = _pick_id(source_campaign, ["campaign_id", "id"])
        if not source_campaign_id:
            raise RuntimeError(f"Source campaign is missing id: {source_campaign}")

        selected_campaign_name = str(target_campaign_name)
        for _ in range(20):
            create_payload = self._build_campaign_create_payload(source_campaign, selected_campaign_name)
            try:
                create_data = self.client.post("/campaign/create/", create_payload)
                new_campaign_id = _pick_id(create_data, ["campaign_id", "new_campaign_id", "ids"])
                if not new_campaign_id:
                    raise RuntimeError(f"Unable to extract campaign_id from /campaign/create/ response: {create_data}")
                new_campaign_id = str(new_campaign_id)
                logging.info(
                    "Fallback duplicate created campaign %s via /campaign/create/ (name=%s).",
                    new_campaign_id,
                    selected_campaign_name,
                )
                break
            except TikTokAPIError as exc:
                if "Campaign name already exists" not in str(exc):
                    raise
                next_name = self._make_unique_campaign_name(selected_campaign_name)
                if next_name == selected_campaign_name:
                    raise RuntimeError(
                        f"Campaign name collision for '{selected_campaign_name}' and no unique suffix available."
                    ) from exc
                logging.warning(
                    "Campaign name '%s' already exists; retrying with '%s'.",
                    selected_campaign_name,
                    next_name,
                )
                selected_campaign_name = next_name
        else:
            raise RuntimeError(
                f"Unable to create a unique campaign name after retries starting from '{target_campaign_name}'."
            )

        source_adgroups = self._get_entities_for_campaign("/adgroup/get/", source_campaign_id, "source_adgroups")
        adgroup_id_map: dict[str, str] = {}
        for source_adgroup in source_adgroups:
            source_adgroup_id = _pick_id(source_adgroup, ["adgroup_id", "id"])
            if not source_adgroup_id:
                logging.warning("Skipping source adgroup without id: %s", source_adgroup)
                continue

            payload = self._build_adgroup_create_payload(source_adgroup, new_campaign_id)
            created = self._create_adgroup_with_retry(payload)
            new_adgroup_id = _pick_id(created, ["adgroup_id", "new_adgroup_id", "ids"])
            if not new_adgroup_id:
                raise RuntimeError(f"Unable to extract adgroup_id from /adgroup/create/ response: {created}")
            adgroup_id_map[str(source_adgroup_id)] = str(new_adgroup_id)
            logging.info("Created adgroup %s from source %s", new_adgroup_id, source_adgroup_id)

        source_ads = self._get_source_ads_for_manual_copy(
            source_campaign_id=source_campaign_id,
            source_adgroups=source_adgroups,
        )
        created_ads = 0
        for source_ad in source_ads:
            source_adgroup_id = _pick_id(source_ad, ["adgroup_id"])
            if not source_adgroup_id or str(source_adgroup_id) not in adgroup_id_map:
                logging.warning("Skipping ad with unknown source adgroup mapping: %s", source_ad.get("ad_id"))
                continue

            payload = self._build_ad_create_payload(
                source_ad=source_ad,
                new_adgroup_id=adgroup_id_map[str(source_adgroup_id)],
                tiktok_item_id=latest_item_id,
            )
            self.client.post("/ad/create/", payload)
            logging.info("Created ad in new adgroup %s from source ad %s", adgroup_id_map[str(source_adgroup_id)], source_ad.get("ad_id"))
            created_ads += 1

        if len(adgroup_id_map) != len(source_adgroups):
            raise RuntimeError(
                f"Created {len(adgroup_id_map)} adgroups but source campaign has {len(source_adgroups)} adgroups."
            )
        if created_ads != len(source_ads):
            raise RuntimeError(f"Created {created_ads} ads but source campaign has {len(source_ads)} ads.")

        return new_campaign_id, selected_campaign_name

    def _make_unique_campaign_name(self, base_name: str) -> str:
        campaigns = self._list_entities("/campaign/get/")
        existing_names = {
            str(campaign.get("campaign_name") or campaign.get("name") or "").strip()
            for campaign in campaigns
            if str(campaign.get("campaign_name") or campaign.get("name") or "").strip()
        }
        if base_name not in existing_names:
            return base_name

        suffix = 1
        while True:
            candidate = f"{base_name}_{suffix}"
            if candidate not in existing_names:
                return candidate
            suffix += 1

    def _get_source_ads_for_manual_copy(
        self,
        source_campaign_id: str,
        source_adgroups: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        initial_ads: list[dict[str, Any]] = []
        root_exc: Exception | None = None
        try:
            initial_ads = self._get_entities_for_campaign("/ad/get/", source_campaign_id, "source_ads")
        except TikTokAPIError as exc:
            lowered = str(exc).lower()
            if "code=40002" not in lowered and "no permission to access this ad" not in lowered:
                raise
            root_exc = exc

            logging.warning(
                "Campaign-level /ad/get/ failed for source campaign %s due permissions (%s). "
                "Retrying source ad fetch adgroup-by-adgroup.",
                source_campaign_id,
                exc,
            )
        else:
            if initial_ads:
                return initial_ads
            logging.warning(
                "Campaign-level /ad/get/ returned 0 ads for source campaign %s. "
                "Retrying source ad fetch adgroup-by-adgroup.",
                source_campaign_id,
            )
            root_exc = RuntimeError("campaign-level /ad/get/ returned no source ads")

        collected: list[dict[str, Any]] = []
        for source_adgroup in source_adgroups:
            source_adgroup_id = _pick_id(source_adgroup, ["adgroup_id", "id"])
            if not source_adgroup_id:
                continue
            try:
                ads = self._get_entities_for_adgroup("/ad/get/", str(source_adgroup_id), "source_ads")
                if ads:
                    logging.info("Loaded %d source ads from adgroup %s.", len(ads), source_adgroup_id)
                collected.extend(ads)
            except TikTokAPIError as sub_exc:
                logging.warning(
                    "Skipping source adgroup %s during ad fetch due API error: %s",
                    source_adgroup_id,
                    sub_exc,
                )

        deduped: dict[str, dict[str, Any]] = {}
        for ad in collected:
            ad_id = _pick_id(ad, ["ad_id", "id"])
            if not ad_id:
                continue
            deduped[str(ad_id)] = ad

        result = list(deduped.values())
        if result:
            return result

        raise RuntimeError(
            "Unable to read source ads for the source campaign. "
            "TikTok returned no accessible ads in both campaign and adgroup fetch paths."
        ) from root_exc

    def _build_campaign_create_payload(
        self, source_campaign: dict[str, Any], target_campaign_name: str
    ) -> dict[str, Any]:
        objective_type = source_campaign.get("objective_type") or source_campaign.get("objective")
        if not objective_type:
            raise RuntimeError(f"Source campaign missing objective/objective_type: {source_campaign}")

        payload: dict[str, Any] = {
            "advertiser_id": self.config.advertiser_id,
            "campaign_name": target_campaign_name,
            "objective_type": objective_type,
            "budget_mode": source_campaign.get("budget_mode") or "BUDGET_MODE_TOTAL",
        }

        budget_optimize_on = source_campaign.get("budget_optimize_on")
        if budget_optimize_on is not None:
            payload["budget_optimize_on"] = bool(budget_optimize_on)

        budget = _to_float(source_campaign.get("budget"))
        if budget > 0:
            payload["budget"] = budget

        return payload

    def _build_adgroup_create_payload(
        self, source_adgroup: dict[str, Any], new_campaign_id: str
    ) -> dict[str, Any]:
        start_time, end_time = _normalized_schedule_window(
            source_adgroup.get("schedule_start_time"), source_adgroup.get("schedule_end_time")
        )

        payload: dict[str, Any] = {
            "advertiser_id": self.config.advertiser_id,
            "campaign_id": str(new_campaign_id),
            "adgroup_name": source_adgroup.get("adgroup_name") or f"adgroup_{new_campaign_id}",
            "placement_type": source_adgroup.get("placement_type") or "PLACEMENT_TYPE_NORMAL",
            "placements": [p for p in source_adgroup.get("placements", []) if p],
            "promotion_type": source_adgroup.get("promotion_type") or "WEBSITE_OR_DISPLAY",
            "budget_mode": source_adgroup.get("budget_mode") or "BUDGET_MODE_INFINITE",
            "schedule_type": source_adgroup.get("schedule_type") or "SCHEDULE_START_END",
            "schedule_start_time": start_time,
            "schedule_end_time": end_time,
            "optimization_goal": source_adgroup.get("optimization_goal"),
            "billing_event": source_adgroup.get("billing_event"),
            "bid_type": source_adgroup.get("bid_type") or "BID_TYPE_NO_BID",
            "pacing": source_adgroup.get("pacing") or "PACING_MODE_SMOOTH",
            "location_ids": source_adgroup.get("location_ids") or [],
            "age_groups": source_adgroup.get("age_groups") or [],
            "gender": source_adgroup.get("gender") or "GENDER_UNLIMITED",
            "creative_material_mode": source_adgroup.get("creative_material_mode") or "CUSTOM",
        }

        budget_mode = str(payload.get("budget_mode") or "")
        if budget_mode in {"BUDGET_MODE_DAY", "BUDGET_MODE_TOTAL", "BUDGET_MODE_DYNAMIC_DAILY_BUDGET"}:
            budget = _to_float(source_adgroup.get("budget"))
            payload["budget"] = max(budget, 1.0)

        if source_adgroup.get("languages") is not None:
            payload["languages"] = source_adgroup.get("languages") or []

        for key in (
            "audience_ids",
            "excluded_audience_ids",
            "interest_category_ids",
            "interest_keyword_ids",
            "actions",
            "included_custom_actions",
            "excluded_custom_actions",
            "household_income",
            "spending_power",
            "contextual_tag_ids",
            "category_exclusion_ids",
            "device_model_ids",
            "device_price_ranges",
            "operating_systems",
            "network_types",
            "isp_ids",
            "inventory_filter_enabled",
            "search_result_enabled",
            "automated_keywords_enabled",
            "smart_audience_enabled",
            "smart_interest_behavior_enabled",
            "tiktok_subplacements",
            "dayparting",
            "frequency",
            "frequency_schedule",
        ):
            value = source_adgroup.get(key)
            if value is None:
                continue
            if isinstance(value, list):
                value = [item for item in value if item is not None and str(item).strip() != ""]
            if isinstance(value, list) and not value:
                continue
            payload[key] = copy.deepcopy(value)

        return payload

    def _create_adgroup_with_retry(self, payload: dict[str, Any], max_attempts: int = 15) -> dict[str, Any]:
        working = copy.deepcopy(payload)
        required_fields = {
            "advertiser_id",
            "campaign_id",
            "adgroup_name",
            "placement_type",
            "placements",
            "promotion_type",
            "budget_mode",
            "schedule_type",
            "schedule_start_time",
            "schedule_end_time",
            "optimization_goal",
            "billing_event",
            "bid_type",
            "pacing",
            "location_ids",
            "age_groups",
            "gender",
            "creative_material_mode",
        }

        for _ in range(max_attempts):
            try:
                return self.client.post("/adgroup/create/", working)
            except TikTokAPIError as exc:
                error_text = str(exc)
                lowered = error_text.lower()

                if "campaign budget optimization" in lowered and "budget_mode" in lowered:
                    working["budget_mode"] = "BUDGET_MODE_DAY"
                    working["budget"] = max(_to_float(working.get("budget")), 1.0)
                    logging.warning("Adjusted adgroup budget_mode to BUDGET_MODE_DAY to satisfy CBO constraints.")
                    continue

                field = _extract_error_field_name(error_text)
                if field:
                    root_field = field.split(".")[0]
                    if root_field in working and root_field not in required_fields:
                        working.pop(root_field, None)
                        logging.warning("Removed unsupported adgroup field '%s' and retrying.", root_field)
                        continue

                raise

        raise RuntimeError(f"Failed to create adgroup after retries. Last payload: {working}")

    def _build_ad_create_payload(
        self, source_ad: dict[str, Any], new_adgroup_id: str, tiktok_item_id: str
    ) -> dict[str, Any]:
        ad_name = source_ad.get("ad_name") or self.config.date_tag
        creative: dict[str, Any] = {
            "ad_name": ad_name,
            "ad_format": source_ad.get("ad_format") or "SINGLE_VIDEO",
            "tiktok_item_id": str(tiktok_item_id),
            "identity_id": source_ad.get("identity_id"),
            "identity_type": source_ad.get("identity_type"),
        }

        payload: dict[str, Any] = {
            "advertiser_id": self.config.advertiser_id,
            "adgroup_id": str(new_adgroup_id),
            "ad_name": ad_name,
            "creatives": [creative],
        }
        return payload

    def _backfill_ads_from_source_campaign(
        self,
        source_campaign_id: str,
        target_adgroups: list[dict[str, Any]],
        tiktok_item_id: str,
    ) -> int:
        source_adgroups = self._get_entities_for_campaign("/adgroup/get/", source_campaign_id, "source_adgroups")
        source_ads = self._get_source_ads_for_manual_copy(
            source_campaign_id=source_campaign_id,
            source_adgroups=source_adgroups,
        )
        if not source_ads:
            raise RuntimeError(f"Source campaign {source_campaign_id} has no accessible ads to backfill.")

        source_adgroups_by_id: dict[str, dict[str, Any]] = {}
        for adgroup in source_adgroups:
            adgroup_id = _pick_id(adgroup, ["adgroup_id", "id"])
            if adgroup_id:
                source_adgroups_by_id[str(adgroup_id)] = adgroup

        # Primary mapping by adgroup_name; fallback to remaining target adgroups.
        target_by_name: dict[str, list[str]] = {}
        all_target_ids: list[str] = []
        for target in target_adgroups:
            target_id = _pick_id(target, ["adgroup_id", "id"])
            if not target_id:
                continue
            all_target_ids.append(str(target_id))
            name = str(target.get("adgroup_name") or "").strip()
            target_by_name.setdefault(name, []).append(str(target_id))

        if not all_target_ids:
            raise RuntimeError("No target adgroups available for ad backfill.")

        source_to_target: dict[str, str] = {}
        used_targets: set[str] = set()
        fallback_index = 0
        for source in source_adgroups:
            source_id = _pick_id(source, ["adgroup_id", "id"])
            if not source_id:
                continue
            source_name = str(source.get("adgroup_name") or "").strip()

            mapped_target = None
            for candidate in target_by_name.get(source_name, []):
                if candidate not in used_targets:
                    mapped_target = candidate
                    break

            while mapped_target is None and fallback_index < len(all_target_ids):
                candidate = all_target_ids[fallback_index]
                fallback_index += 1
                if candidate not in used_targets:
                    mapped_target = candidate
                    break

            if mapped_target is None:
                continue

            source_to_target[str(source_id)] = mapped_target
            used_targets.add(mapped_target)

        created = 0
        for source_ad in source_ads:
            source_adgroup_id = _pick_id(source_ad, ["adgroup_id"])
            if not source_adgroup_id:
                continue

            target_adgroup_id = source_to_target.get(str(source_adgroup_id))
            if not target_adgroup_id and all_target_ids:
                source_group = source_adgroups_by_id.get(str(source_adgroup_id), {})
                source_name = str(source_group.get("adgroup_name") or "").strip()
                for candidate in target_by_name.get(source_name, []):
                    target_adgroup_id = candidate
                    break
            if not target_adgroup_id and all_target_ids:
                target_adgroup_id = all_target_ids[0]
            if not target_adgroup_id:
                logging.warning("Skipping source ad without target adgroup mapping: %s", source_ad.get("ad_id"))
                continue

            payload = self._build_ad_create_payload(
                source_ad=source_ad,
                new_adgroup_id=str(target_adgroup_id),
                tiktok_item_id=tiktok_item_id,
            )
            self.client.post("/ad/create/", payload)
            created += 1

        return created

    def _wait_for_ads_count(
        self,
        campaign_id: str,
        min_count: int,
        max_attempts: int = 6,
        sleep_seconds: int = 3,
    ) -> list[dict[str, Any]]:
        last_ads: list[dict[str, Any]] = []
        for attempt in range(1, max_attempts + 1):
            last_ads = self._get_entities_for_campaign("/ad/get/", campaign_id, "ads")
            if len(last_ads) >= max(0, int(min_count)):
                return last_ads
            if attempt < max_attempts:
                time.sleep(sleep_seconds)
        return last_ads

    def _update_campaign(self, campaign_id: str, patch: dict[str, Any]) -> None:
        payload = {
            "advertiser_id": self.config.advertiser_id,
            "campaign_id": str(campaign_id),
            **patch,
        }
        self.client.post("/campaign/update/", payload)

    def _get_entities_for_campaign(
        self, endpoint: str, campaign_id: str, entity_label: str
    ) -> list[dict[str, Any]]:
        if endpoint in {"/ad/get/", "/adgroup/get/"}:
            campaign_filtered = self._try_list_entities_filtered(
                endpoint=endpoint,
                filtering_candidates=[
                    {"campaign_ids": [str(campaign_id)]},
                    [{"field_name": "campaign_id", "filter_type": "IN", "filter_value": [str(campaign_id)]}],
                    [{"field_name": "campaign_id", "filter_type": "IN", "filter_value": str(campaign_id)}],
                ],
            )
            if campaign_filtered is not None:
                filtered = [e for e in campaign_filtered if str(e.get("campaign_id")) == str(campaign_id)]
                return filtered if filtered else campaign_filtered

        entities = self._list_entities(endpoint)
        filtered = [e for e in entities if str(e.get("campaign_id")) == str(campaign_id)]
        if not filtered:
            logging.warning(
                "No %s matched campaign_id=%s. Endpoint returned %d total %s.",
                entity_label,
                campaign_id,
                len(entities),
                entity_label,
            )
        return filtered

    def _get_entities_for_adgroup(
        self, endpoint: str, adgroup_id: str, entity_label: str
    ) -> list[dict[str, Any]]:
        if endpoint == "/ad/get/":
            adgroup_filtered = self._try_list_entities_filtered(
                endpoint=endpoint,
                filtering_candidates=[
                    {"adgroup_ids": [str(adgroup_id)]},
                    [{"field_name": "adgroup_id", "filter_type": "IN", "filter_value": [str(adgroup_id)]}],
                    [{"field_name": "adgroup_id", "filter_type": "IN", "filter_value": str(adgroup_id)}],
                ],
            )
            if adgroup_filtered is not None:
                filtered = [e for e in adgroup_filtered if str(e.get("adgroup_id")) == str(adgroup_id)]
                return filtered if filtered else adgroup_filtered

        entities = self._list_entities(endpoint)
        filtered = [e for e in entities if str(e.get("adgroup_id")) == str(adgroup_id)]
        if not filtered:
            logging.warning(
                "No %s matched adgroup_id=%s. Endpoint returned %d total %s.",
                entity_label,
                adgroup_id,
                len(entities),
                entity_label,
            )
        return filtered

    def _try_list_entities_filtered(
        self,
        endpoint: str,
        filtering_candidates: list[Any],
    ) -> list[dict[str, Any]] | None:
        for candidate in filtering_candidates:
            params = {
                "advertiser_id": self.config.advertiser_id,
                "filtering": json.dumps(candidate, separators=(",", ":")),
            }
            try:
                return self._list_entities_with_params(endpoint, params)
            except TikTokAPIError as exc:
                logging.debug(
                    "Filtered %s lookup failed for candidate %s: %s",
                    endpoint,
                    candidate,
                    exc,
                )
                continue
        return None

    def _clear_adgroup_bids(self, adgroups: list[dict[str, Any]]) -> list[str]:
        cleared: list[str] = []

        for adgroup in adgroups:
            adgroup_id = _pick_id(adgroup, ["adgroup_id", "id"])
            if not adgroup_id:
                logging.warning("Skipping ad group without id: %s", adgroup)
                continue

            bid_price_value = _to_float(adgroup.get("bid_price"))
            if bid_price_value <= 0:
                continue

            payload = {
                "advertiser_id": self.config.advertiser_id,
                "adgroup_id": str(adgroup_id),
                "bid_price": 0,
            }
            if adgroup.get("bid_type"):
                payload["bid_type"] = adgroup["bid_type"]

            self.client.post("/adgroup/update/", payload)
            cleared.append(str(adgroup_id))
            logging.info("Cleared bid for adgroup %s", adgroup_id)

        return cleared

    def _update_ads(self, ads: list[dict[str, Any]], ad_name: str, tiktok_item_id: str) -> list[str]:
        updated: list[str] = []

        for ad in ads:
            ad_id = _pick_id(ad, ["ad_id", "id"])
            adgroup_id = _pick_id(ad, ["adgroup_id"])
            if not ad_id:
                logging.warning("Skipping ad without id: %s", ad)
                continue
            if not adgroup_id:
                logging.warning("Skipping ad without adgroup_id: %s", ad_id)
                continue

            payload: dict[str, Any] = {
                "advertiser_id": self.config.advertiser_id,
                "ad_id": str(ad_id),
                "adgroup_id": str(adgroup_id),
                "ad_name": ad_name,
            }

            normalized_creatives = [{
                "ad_id": str(ad_id),
                "ad_name": ad_name,
                "ad_format": ad.get("ad_format") or "SINGLE_VIDEO",
                "identity_id": ad.get("identity_id"),
                "identity_type": ad.get("identity_type"),
                "tiktok_item_id": str(tiktok_item_id),
            }]

            payload["creatives"] = normalized_creatives

            self.client.post("/ad/update/", payload)
            updated.append(str(ad_id))
            logging.info("Updated ad %s", ad_id)

        return updated

    def _ensure_ads_alignment(
        self,
        campaign_id: str,
        ad_name: str,
        tiktok_item_id: str,
        max_attempts: int = 6,
        sleep_seconds: int = 5,
    ) -> dict[str, Any]:
        fixed_ad_ids: set[str] = set()
        mismatched_ad_ids: list[str] = []
        target_item_id = str(tiktok_item_id)

        for attempt in range(1, max_attempts + 1):
            ads = self._get_entities_for_campaign("/ad/get/", campaign_id, "ads")
            mismatched: list[dict[str, Any]] = []

            for ad in ads:
                current_name = str(ad.get("ad_name") or "").strip()
                current_item_id = str(ad.get("tiktok_item_id") or "").strip()
                if current_name != ad_name or current_item_id != target_item_id:
                    mismatched.append(ad)

            if not mismatched:
                return {
                    "attempts": attempt,
                    "ads_seen": len(ads),
                    "ads_fixed": len(fixed_ad_ids),
                    "mismatched_ad_ids": [],
                }

            logging.info(
                "Ad alignment attempt %d/%d: %d mismatched ads; applying corrective updates.",
                attempt,
                max_attempts,
                len(mismatched),
            )

            mismatched_ad_ids = []
            for ad in mismatched:
                ad_id = _pick_id(ad, ["ad_id", "id"])
                adgroup_id = _pick_id(ad, ["adgroup_id"])
                if not ad_id or not adgroup_id:
                    continue
                mismatched_ad_ids.append(str(ad_id))

                payload: dict[str, Any] = {
                    "advertiser_id": self.config.advertiser_id,
                    "ad_id": str(ad_id),
                    "adgroup_id": str(adgroup_id),
                    "ad_name": ad_name,
                    "creatives": [{
                        "ad_id": str(ad_id),
                        "ad_name": ad_name,
                        "ad_format": ad.get("ad_format") or "SINGLE_VIDEO",
                        "identity_id": ad.get("identity_id"),
                        "identity_type": ad.get("identity_type"),
                        "tiktok_item_id": target_item_id,
                    }],
                }

                try:
                    self.client.post("/ad/update/", payload)
                    fixed_ad_ids.add(str(ad_id))
                except TikTokAPIError as exc:
                    logging.warning(
                        "Corrective /ad/update/ with creatives failed for ad %s (%s). Retrying rename-only.",
                        ad_id,
                        exc,
                    )
                    self.client.post(
                        "/ad/update/",
                        {
                            "advertiser_id": self.config.advertiser_id,
                            "ad_id": str(ad_id),
                            "adgroup_id": str(adgroup_id),
                            "ad_name": ad_name,
                        },
                    )
                    fixed_ad_ids.add(str(ad_id))

            if attempt < max_attempts:
                time.sleep(sleep_seconds)

        return {
            "attempts": max_attempts,
            "ads_seen": len(self._get_entities_for_campaign("/ad/get/", campaign_id, "ads")),
            "ads_fixed": len(fixed_ad_ids),
            "mismatched_ad_ids": mismatched_ad_ids,
        }

    def _enable_adgroups(self, adgroups: list[dict[str, Any]]) -> int:
        ids: list[str] = []
        for adgroup in adgroups:
            adgroup_id = _pick_id(adgroup, ["adgroup_id", "id"])
            if not adgroup_id:
                continue
            ids.append(str(adgroup_id))
        if not ids:
            return 0
        self.client.post(
            "/adgroup/status/update/",
            {
                "advertiser_id": self.config.advertiser_id,
                "adgroup_ids": ids,
                "operation_status": "ENABLE",
                "allow_partial_success": True,
            },
        )
        return len(ids)

    def _enable_ads(self, ads: list[dict[str, Any]]) -> int:
        ids: list[str] = []
        for ad in ads:
            ad_id = _pick_id(ad, ["ad_id", "id"])
            if not ad_id:
                continue
            ids.append(str(ad_id))
        if not ids:
            return 0
        self.client.post(
            "/ad/status/update/",
            {
                "advertiser_id": self.config.advertiser_id,
                "ad_ids": ids,
                "operation_status": "ENABLE",
            },
        )
        return len(ids)

    def _set_campaign_status(self, campaign_ids: list[str], operation_status: str) -> None:
        if not campaign_ids:
            return
        self.client.post(
            "/campaign/status/update/",
            {
                "advertiser_id": self.config.advertiser_id,
                "campaign_ids": [str(campaign_id) for campaign_id in campaign_ids],
                "operation_status": operation_status,
            },
        )

    def _list_entities(self, endpoint: str) -> list[dict[str, Any]]:
        return self._list_entities_with_params(
            endpoint=endpoint,
            base_params={"advertiser_id": self.config.advertiser_id},
        )

    def _list_entities_with_params(self, endpoint: str, base_params: dict[str, Any]) -> list[dict[str, Any]]:
        try:
            first_page = self.client.get(endpoint, {**base_params, "page": 1, "page_size": 100})
            used_pagination = True
        except TikTokAPIError as exc:
            error_text = str(exc).lower()
            if "page" in error_text or "page_size" in error_text:
                first_page = self.client.get(endpoint, base_params)
                used_pagination = False
            else:
                raise

        collected = _extract_list(first_page)

        if not used_pagination:
            return _coerce_dict_list(collected)

        page_info = first_page.get("page_info") if isinstance(first_page, dict) else None
        total_number = None
        if isinstance(page_info, dict):
            total_number = _to_int(page_info.get("total_number"))

        page = 2
        while collected and (total_number is None or len(collected) < total_number):
            page_data = self.client.get(endpoint, {**base_params, "page": page, "page_size": 100})
            page_items = _extract_list(page_data)
            if not page_items:
                break

            collected.extend(page_items)
            if len(page_items) < 100:
                break
            page += 1

        return _coerce_dict_list(collected)


def _extract_list(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in (
            "list",
            "campaigns",
            "adgroups",
            "ads",
            "posts",
            "items",
            "results",
            "data",
        ):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def _coerce_dict_list(items: list[Any]) -> list[dict[str, Any]]:
    return [item for item in items if isinstance(item, dict)]


def _parse_time_value(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        # Handle milliseconds timestamps.
        if value > 10_000_000_000:
            return float(value) / 1000.0
        return float(value)

    string_value = str(value).strip()
    if not string_value:
        return 0.0

    if string_value.isdigit():
        numeric = float(string_value)
        if numeric > 10_000_000_000:
            return numeric / 1000.0
        return numeric

    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ):
        try:
            return dt.datetime.strptime(string_value, fmt).timestamp()
        except ValueError:
            continue

    return 0.0


def _normalized_schedule_window(
    source_start: Any, source_end: Any, minimum_minutes_from_now: int = 2, default_days: int = 2
) -> tuple[str, str]:
    now = dt.datetime.now().replace(second=0, microsecond=0)
    min_start = now + dt.timedelta(minutes=minimum_minutes_from_now)

    parsed_start = _parse_datetime_value(source_start)
    parsed_end = _parse_datetime_value(source_end)

    start = max(parsed_start or min_start, min_start)
    if parsed_end and parsed_end > start:
        end = parsed_end
    else:
        end = start + dt.timedelta(days=default_days)

    return start.strftime("%Y-%m-%d %H:%M:%S"), end.strftime("%Y-%m-%d %H:%M:%S")


def _parse_datetime_value(value: Any) -> dt.datetime | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        try:
            return dt.datetime.fromtimestamp(numeric)
        except (OSError, OverflowError, ValueError):
            return None

    string_value = str(value).strip()
    if not string_value:
        return None
    if string_value.isdigit():
        numeric = float(string_value)
        if numeric > 10_000_000_000:
            numeric /= 1000.0
        try:
            return dt.datetime.fromtimestamp(numeric)
        except (OSError, OverflowError, ValueError):
            return None

    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            return dt.datetime.strptime(string_value, fmt)
        except ValueError:
            continue
    return None


def _pick_id(source: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        value = source.get(key)
        if isinstance(value, list) and value:
            value = value[0]
        if value is not None and str(value).strip():
            return str(value)
    return None


def _extract_post_item_id(post: dict[str, Any]) -> str | None:
    direct = (
        post.get("item_id")
        or post.get("tiktok_item_id")
        or post.get("post_id")
    )
    if direct is not None and str(direct).strip():
        return str(direct)

    item_info = post.get("item_info")
    if isinstance(item_info, dict):
        nested = (
            item_info.get("item_id")
            or item_info.get("tiktok_item_id")
            or item_info.get("post_id")
        )
        if nested is not None and str(nested).strip():
            return str(nested)

    return None


def _extract_post_time(post: dict[str, Any]) -> float:
    direct = (
        post.get("post_time")
        or post.get("create_time")
        or post.get("publish_time")
        or post.get("create_timestamp")
        or post.get("publish_timestamp")
        or post.get("create_time_ms")
        or post.get("publish_time_ms")
    )
    direct_value = _parse_time_value(direct)
    if direct_value > 0:
        return direct_value

    item_info = post.get("item_info")
    if isinstance(item_info, dict):
        nested = (
            item_info.get("post_time")
            or item_info.get("create_time")
            or item_info.get("publish_time")
            or item_info.get("create_timestamp")
            or item_info.get("publish_timestamp")
            or item_info.get("create_time_ms")
            or item_info.get("publish_time_ms")
        )
        nested_value = _parse_time_value(nested)
        if nested_value > 0:
            return nested_value

    return 0.0


def _extract_ad_auth_status(post: dict[str, Any]) -> str:
    auth_info = post.get("auth_info")
    if isinstance(auth_info, dict):
        value = auth_info.get("ad_auth_status")
        if value is not None:
            return str(value).strip().upper()
    return ""


def _item_id_sort_key(item_id: str) -> float:
    try:
        return float(int(item_id))
    except (TypeError, ValueError):
        return 0.0


def _extract_error_field_name(error_text: str) -> str | None:
    if not error_text:
        return None
    patterns = [
        r"Invalid param:\s*([A-Za-z0-9_\.]+)",
        r"Invalid parameters?:\s*([A-Za-z0-9_\.]+)",
        r"([A-Za-z0-9_\.]+): Missing data for required field",
        r"([A-Za-z0-9_\.]+): Field may not be null",
    ]
    for pattern in patterns:
        match = re.search(pattern, error_text)
        if match:
            return match.group(1)
    return None


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _env_truthy(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = str(raw).strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _build_campaign_manager_url(advertiser_id: str, campaign_id: str | None) -> str:
    params: dict[str, str] = {"aadvid": str(advertiser_id)}
    if campaign_id:
        params["campaign_id"] = str(campaign_id)
    return "https://ads.tiktok.com/i18n/perf/campaign?" + urllib.parse.urlencode(params)


def _coerce_tiktok_item_id(value: str) -> str:
    text = _normalize_input_link(value)
    if not text:
        raise RuntimeError("Empty TikTok item id override.")

    extracted = _extract_item_id_from_text(text)
    if extracted:
        return extracted

    direct = re.fullmatch(r"\d+", text)
    if direct:
        return text

    parsed = urllib.parse.urlparse(text)
    is_url = parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    if is_url and _is_tiktok_host(parsed.netloc):
        try:
            resolved_url, response_text = _resolve_tiktok_url(text)
        except RuntimeError as exc:
            logging.warning("Could not resolve TikTok URL '%s': %s", text, exc)
        else:
            for candidate in (resolved_url, response_text):
                extracted = _extract_item_id_from_text(candidate)
                if extracted:
                    return extracted

    raise RuntimeError(
        "Invalid --latest-item-id value. Provide a numeric item id or a TikTok video URL "
        "(example: https://www.tiktok.com/@user/video/1234567890). "
        "Short links like https://vm.tiktok.com/... are supported."
    )


def _normalize_input_link(value: str) -> str:
    text = html.unescape(str(value).strip())
    if not text:
        return ""

    urls = _extract_urls_from_text(text)
    if urls:
        for candidate in urls:
            parsed = urllib.parse.urlparse(candidate)
            if _is_tiktok_host(parsed.netloc):
                return candidate
        return urls[0]

    if text.startswith("<") and text.endswith(">"):
        text = text[1:-1].strip()
    if text.startswith("http") and "|" in text:
        text = text.split("|", 1)[0].strip()
    return text


def _is_tiktok_host(host: str) -> bool:
    host = str(host or "").lower()
    return host == "tiktok.com" or host.endswith(".tiktok.com")


def _extract_urls_from_text(text: str) -> list[str]:
    urls: list[str] = []
    for raw in re.findall(r"https?://[^\s<>]+", str(text)):
        candidate = raw.strip(" \t\r\n<>()[]{}.,;\"'")
        if not candidate:
            continue
        if "|" in candidate:
            candidate = candidate.split("|", 1)[0].strip()
        urls.append(candidate)
    return urls


def _extract_item_id_from_text(text: str | None) -> str | None:
    if not text:
        return None
    raw = str(text)

    patterns = [
        r"/video/(\d+)",
        r"/v/(\d+)\.html",
        r'"itemId"\s*:\s*"(\d+)"',
        r'"item_id"\s*:\s*"(\d+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, raw)
        if match:
            return match.group(1)

    parsed = urllib.parse.urlparse(raw)
    if parsed.scheme and parsed.netloc:
        query = urllib.parse.parse_qs(parsed.query)
        if parsed.fragment and "=" in parsed.fragment:
            fragment_qs = urllib.parse.parse_qs(parsed.fragment)
            for key, values in fragment_qs.items():
                query.setdefault(key, values)
        for key in ("item_id", "share_item_id", "video_id", "aweme_id", "group_id"):
            values = query.get(key) or []
            for value in values:
                stripped = str(value).strip()
                if stripped.isdigit():
                    return stripped

    return None


def _resolve_tiktok_url(url: str, timeout_seconds: int = 20) -> tuple[str, str]:
    request = urllib.request.Request(
        url=url,
        method="GET",
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
    )

    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            final_url = response.geturl()
            body_bytes = response.read(512_000)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body[:200]}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"network error: {exc}") from exc

    try:
        body = body_bytes.decode("utf-8", errors="replace")
    except Exception:
        body = ""
    return final_url, body


def _post_slack_message(slack_token: str, channel_id: str, text: str) -> None:
    payload = {"channel": channel_id, "text": text}
    request = urllib.request.Request(
        url="https://slack.com/api/chat.postMessage",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {slack_token}",
            "Content-Type": "application/json; charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        err = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Slack HTTP {exc.code}: {err}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Slack network error: {exc}") from exc

    try:
        parsed = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Slack returned invalid JSON: {body[:300]}") from exc

    if not isinstance(parsed, dict) or not parsed.get("ok"):
        raise RuntimeError(f"Slack API error: {parsed}")


def _notify_slack_summary(result: dict[str, Any], config: AutomationConfig, channel_id: str) -> None:
    slack_token = os.getenv("SLACK_BOT_TOKEN")
    if not slack_token:
        logging.info("SLACK_BOT_TOKEN not set; skipping Slack notification.")
        return

    campaign_id = str(result.get("new_campaign_id") or "").strip() or None
    campaign_name = str(result.get("new_campaign_name") or "").strip() or "(unknown)"
    manage_url = os.getenv("TIKTOK_MANAGE_CAMPAIGN_URL", DEFAULT_TIKTOK_MANAGE_CAMPAIGN_URL)

    message = (
        "TikTok Boost Automation Completed\n"
        f"Campaign: {campaign_name}\n"
        f"Manage Campaigns: <{manage_url}|Open manage campaigns>"
    )

    _post_slack_message(slack_token=slack_token, channel_id=channel_id, text=message)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Automate TikTok campaign duplication and Spark creative refresh.")
    parser.add_argument(
        "--base-url",
        default=os.getenv("TIKTOK_BASE_URL", DEFAULT_BASE_URL),
        help=f"TikTok Business API base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--advertiser-id",
        default=os.getenv("TIKTOK_ADVERTISER_ID", DEFAULT_ADVERTISER_ID),
        help=f"Advertiser ID (default: {DEFAULT_ADVERTISER_ID})",
    )
    parser.add_argument(
        "--campaign-name-substring",
        default=os.getenv("TIKTOK_CAMPAIGN_NAME_SUBSTRING", DEFAULT_CAMPAIGN_SUBSTRING),
        help=f"Substring used to identify source campaign (default: {DEFAULT_CAMPAIGN_SUBSTRING})",
    )
    parser.add_argument(
        "--source-campaign-name",
        default=os.getenv("TIKTOK_SOURCE_CAMPAIGN_NAME"),
        help="Optional exact source campaign name to copy (overrides substring lookup).",
    )
    parser.add_argument(
        "--post-list-endpoints",
        default=os.getenv("TIKTOK_POST_LIST_ENDPOINTS", DEFAULT_POST_LIST_ENDPOINTS),
        help=(
            "Comma-separated post list endpoints to try for latest item ID "
            f"(default: {DEFAULT_POST_LIST_ENDPOINTS})"
        ),
    )
    parser.add_argument(
        "--latest-item-id",
        default=os.getenv("TIKTOK_LATEST_ITEM_ID"),
        help=(
            "Optional explicit TikTok item ID to use instead of fetching the latest post."
        ),
    )
    parser.add_argument(
        "--video-auth-code",
        default=os.getenv("TIKTOK_VIDEO_AUTH_CODE"),
        help=(
            "Optional TikTok video auth code to call /tt_video/authorize/ before fetching latest post."
        ),
    )
    parser.add_argument(
        "--allow-ad-item-fallback",
        action="store_true",
        default=_env_truthy("TIKTOK_ALLOW_AD_ITEM_FALLBACK", False),
        help=(
            "If post endpoints return no usable latest post, fallback to highest tiktok_item_id "
            "found in existing ads (disabled by default)."
        ),
    )
    parser.add_argument(
        "--slack-channel-id",
        default=os.getenv("SLACK_CHANNEL_ID", DEFAULT_SLACK_CHANNEL_ID),
        help=f"Slack channel id for completion message (default: {DEFAULT_SLACK_CHANNEL_ID})",
    )
    parser.add_argument(
        "--date-tag",
        default=dt.datetime.now().strftime("%Y_%m_%d"),
        help="Date tag used in campaign/ad naming (default: today in YYYY_MM_DD)",
    )
    parser.add_argument(
        "--no-enable",
        action="store_true",
        help="Skip final ENABLE calls for campaign, ad groups, and ads.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Read-only mode: discover source campaign and latest post, then exit before writes.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logs.",
    )
    return parser


def main(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    token = os.getenv("TIKTOK_ACCESS_TOKEN")
    if not token:
        parser.error("Missing TIKTOK_ACCESS_TOKEN environment variable.")

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    client = TikTokBusinessClient(base_url=args.base_url, access_token=token)
    config = AutomationConfig(
        advertiser_id=str(args.advertiser_id),
        campaign_name_substring=args.campaign_name_substring,
        source_campaign_name_exact=args.source_campaign_name,
        date_tag=args.date_tag,
        enable_entities=not args.no_enable,
        dry_run=args.dry_run,
        post_list_endpoints=[endpoint.strip() for endpoint in str(args.post_list_endpoints).split(",") if endpoint.strip()],
        latest_item_id_override=args.latest_item_id,
        video_auth_code=args.video_auth_code,
        allow_ad_item_fallback=bool(args.allow_ad_item_fallback),
    )

    automation = TikTokBoostAutomation(client=client, config=config)

    try:
        result = automation.run()
    except (TikTokAPIError, RuntimeError) as exc:
        logging.error("Automation failed: %s", exc)
        return 1

    if not bool(result.get("dry_run")):
        try:
            _notify_slack_summary(result=result, config=config, channel_id=str(args.slack_channel_id))
        except RuntimeError as exc:
            logging.warning("Slack notification failed: %s", exc)
    else:
        logging.info("Dry-run mode: skipping Slack notification.")

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
