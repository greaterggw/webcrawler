import pandas as pd
import re
import time
import urllib.parse
from concurrent.futures import ProcessPoolExecutor

import traceback
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException, TimeoutException


# --- BEGIN WORKER-COMPATIBLE HELPER FUNCTIONS (Static or Top-Level) ---
# These are direct static translations of your original class methods.

def clean_filename_static(text):  # From original
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[-\s]+', '_', text)
    return text.lower()


def keyword_match_static(text_to_search, keywords_list):  # From original
    if not keywords_list: return False
    text_to_search_lower = text_to_search.lower()
    for keyword_item in keywords_list:
        if keyword_item.lower() not in text_to_search_lower:
            return False
    return True

# Add this new function at the top with other static helper functions

def get_table_headers_and_data_start_idx_static(table_element, all_rows, verbose_val, context=""):
    """
    Tries to extract column headers from a table and determine the starting row index for data.
    Args:
        table_element: The Selenium WebElement for the table.
        all_rows: A list of all <tr> WebElements from the table.
        verbose_val: Verbosity flag.
        context: Logging context.
    Returns:
        tuple: (list_of_header_strings, data_start_row_index)
    """
    headers = []
    data_start_idx = 0  # Default: assume data starts from the first row

    if not all_rows:
        if verbose_val: print(f"[{context}] No rows in table to extract headers from.")
        return [], 0

    # Attempt 1: Check <thead> for <th> elements
    try:
        thead = table_element.find_element(By.TAG_NAME, "thead")
        thead_rows = thead.find_elements(By.TAG_NAME, "tr")
        if thead_rows:
            # Use the last row in <thead> as the primary source of headers
            # More complex logic would be needed for multi-row headers in thead or colspan/rowspan
            header_cells_in_thead = thead_rows[-1].find_elements(By.TAG_NAME, "th")
            if not header_cells_in_thead: # Fallback to <td> in last thead row
                header_cells_in_thead = thead_rows[-1].find_elements(By.TAG_NAME, "td")

            if header_cells_in_thead:
                headers = [cell.text.strip() if cell.text.strip() else f"HeadCol{i+1}" for i, cell in enumerate(header_cells_in_thead)]
                # Data typically starts after the thead. The number of rows in thead is the offset.
                data_start_idx = len(thead_rows)
                if verbose_val: print(f"[{context}] Headers from <thead>: {headers}. Data starts at table row index {data_start_idx}.")
                return headers, data_start_idx
    except NoSuchElementException:
        if verbose_val: print(f"[{context}] No <thead> found.")
    except Exception as e_thead:
        if verbose_val: print(f"[{context}] Error processing <thead>: {e_thead}")


    # Attempt 2: If no headers from <thead>, check the first row of the table for <th>
    first_row_th_cells = all_rows[0].find_elements(By.TAG_NAME, "th")
    if first_row_th_cells:
        headers = [cell.text.strip() if cell.text.strip() else f"Row1Col{i+1}" for i, cell in enumerate(first_row_th_cells)]
        data_start_idx = 1  # Data starts from the second row
        if verbose_val: print(f"[{context}] Headers from first row <th>: {headers}. Data starts at index 1.")
        return headers, data_start_idx

    # Attempt 3 (Heuristic): If still no headers, check if the first row's <td>s look like headers.
    # This is optional and can be risky if the first data row has non-numeric textual data.
    # For now, we'll assume if no <th> are found anywhere, there are no explicit headers to extract this way.
    # The calling functions will then generate generic "Col X" if this list remains empty.
    if not headers and verbose_val:
        print(f"[{context}] No definitive <th> headers found in <thead> or first row. Generic headers will be used if needed.")

    return headers, data_start_idx # data_start_idx might still be 0 if no clear header row identified


def wait_for_page_load_static(driver, wait_time_val, verbose_val, context=""):  # From original _wait_for_page_load
    try:
        if verbose_val: print(f"[{context}] Waiting for RightContent...")
        WebDriverWait(driver, wait_time_val).until(
            EC.presence_of_element_located((By.ID, "RightContent"))
        )
        if verbose_val: print(f"[{context}] Waiting for .tabtitles...")
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".tabtitles"))
        )
        if verbose_val: print(f"[{context}] Page elements loaded.")
    except TimeoutException as e:
        if verbose_val: print(f"[{context}] Timed out waiting for page elements: {e}")
        raise
    except Exception as e:
        if verbose_val: print(f"[{context}] Error waiting for page load: {e}")
        raise


def expand_all_sections_static(driver, verbose_val,
                               context=""):  # Based on original _expand_all_sections (not provided but standard)
    try:
        expand_all_link = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Expand All"))
        )
        driver.execute_script("arguments[0].click();", expand_all_link)
        if verbose_val: print(f"[{context}] Clicked Expand All")
        time.sleep(3)
    except TimeoutException:
        if verbose_val: print(f"[{context}] Expand All link not found or not clickable in time.")
    except Exception as e:
        if verbose_val: print(f"[{context}] Expand All link not found or failed to click: {e}")


def determine_section_static(driver, element, section_titles, verbose_val, context=""):  # From your _determine_section
    try:
        elem_y = driver.execute_script(
            "return arguments[0].getBoundingClientRect().top + window.scrollY;", element
            # Added window.scrollY for absolute pos
        )
    except Exception as e_y:
        if verbose_val: print(f"[{context}] Error getting element Y position: {e_y}")
        return "Unknown Section (JS Error)"

    closest = "Unknown Section (No Match)"  # Default
    best_dist = float('inf')

    if not section_titles:  # Check if section_titles list is empty
        if verbose_val: print(f"[{context}] No section_titles provided for determining section.")
        return "Unknown Section (No Titles Provided)"

    page_headers = driver.find_elements(By.CSS_SELECTOR, ".tabtitles")
    if not page_headers:
        if verbose_val: print(f"[{context}] No .tabtitles headers found on page for determining section.")
        return "Unknown Section (No Headers On Page)"

    for header in page_headers:
        title = header.text.strip()
        if title not in section_titles:  # Only consider titles we've pre-identified as valid
            continue
        try:
            hdr_y = driver.execute_script(
                "return arguments[0].getBoundingClientRect().top + window.scrollY;", header
            )
        except Exception as e_hdr:
            if verbose_val: print(f"[{context}] Error getting header Y position for '{title}': {e_hdr}")
            continue
        dist = elem_y - hdr_y
        if 0 <= dist < best_dist:  # Element is at or below this header and closer than any previous
            best_dist = dist
            closest = title

    if verbose_val and closest == "Unknown Section (No Match)":
        # This log helps debug if sections are not being matched as expected
        header_texts_on_page = [h.text.strip() for h in page_headers]
        print(
            f"[{context}] determine_section_static: Element Y-pos {elem_y}. No valid preceding header found. Headers on page: {header_texts_on_page}. Valid titles provided: {section_titles}")

    return closest


def extract_general_info_static(driver, verbose_val, context=""):  # From your _extract_general_info
    worker_institution_info_dict = {}  # Local dict for this function
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "dashboard")))
        header = driver.find_element(By.CLASS_NAME, "headerlg")
        worker_institution_info_dict["Institution Name"] = header.text.strip()
        try:
            parent_element = header.find_element(By.XPATH, "./..")
            full_text = driver.execute_script("return arguments[0].textContent;", parent_element)
            location_text = full_text.replace(worker_institution_info_dict["Institution Name"], "").strip()
            location_text = re.sub(r"UnitID:\s*\d+", "", location_text).strip().split('\n')[0].strip()
            worker_institution_info_dict["Location"] = location_text if location_text else "Not found"
        except Exception:
            worker_institution_info_dict["Location"] = "Not found (error parsing parent)"

        info_table = driver.find_element(By.CLASS_NAME, "layouttab")
        rows = info_table.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            try:
                label_cells = row.find_elements(By.CLASS_NAME, "srb")
                if not label_cells: continue
                label = label_cells[0].text.strip().replace(":", "").strip()
                all_td_cells = row.find_elements(By.TAG_NAME, "td")
                value = ""
                # Find the actual td cell that is the label_cell and get the next one
                label_cell_index = -1
                for idx, tc in enumerate(all_td_cells):
                    if tc == label_cells[0]:
                        label_cell_index = idx
                        break
                if label_cell_index != -1 and label_cell_index + 1 < len(all_td_cells):
                    value = all_td_cells[label_cell_index + 1].text.strip()
                elif len(all_td_cells) > 1 and all_td_cells[0] != label_cells[
                    0]:  # Fallback if label is in srb but not the first td
                    value = all_td_cells[0].text.strip()  # This was in your original, check if it's right
                elif len(all_td_cells) == 2 and label_cells[0] == all_td_cells[0]:  # Label is the first TD
                    value = all_td_cells[1].text.strip()

                worker_institution_info_dict[label] = value
            except Exception:
                continue

        ipeds_elements = driver.find_elements(By.CLASS_NAME, "ipeds")
        if ipeds_elements:
            ipeds_text = ipeds_elements[0].text.strip()
            ipeds_match = re.search(r"(?:IPEDS ID|UnitID):\s*(\d+)",
                                    ipeds_text)  # Original used IPEDS ID:, site uses UnitID:
            if ipeds_match: worker_institution_info_dict["IPEDS ID"] = ipeds_match.group(1)
            ope_match = re.search(r"OPE ID:\s*(\d+)", ipeds_text)
            if ope_match: worker_institution_info_dict["OPE ID"] = ope_match.group(1)
        if verbose_val: print(
            f"[{context}] General info extracted: {worker_institution_info_dict.get('Institution Name', 'Unknown')}")
    except Exception as e:
        if verbose_val: print(f"[{context}] Error extracting general info: {e}")
    return worker_institution_info_dict


def add_general_info_to_data_static(worker_all_data_list, worker_institution_info_dict, search_terms_list, verbose_val,
                                    context=""):  # From your _add_general_info_to_data
    if not worker_institution_info_dict: return

    # Original logic: if search_terms (passed as search_terms_list) exists, filter by it.
    # If search_terms_list is empty or None, it implies no filtering, add all general info.
    should_add_section = True  # Default to True if no search terms for filtering
    if search_terms_list:  # Only filter if search_terms_list is not empty
        should_add_section = False  # Assume no match until found
        for key, value in worker_institution_info_dict.items():
            key_lower = key.lower()
            value_lower = str(value).lower()
            for term in search_terms_list:
                if term.lower() in key_lower or term.lower() in value_lower:
                    should_add_section = True
                    break
            if should_add_section: break

    if not should_add_section:
        if verbose_val: print(f"[{context}] General info did not match search terms. Skipping add.")
        return

    worker_all_data_list.append({
        "Category": "GENERAL INFORMATION", "Value": "",
        "Section": "General Information", "Source": "Institution Dashboard"
    })
    for key, value in worker_institution_info_dict.items():
        worker_all_data_list.append({
            "Category": key, "Value": value,
            "Section": "General Information", "Source": "Institution Dashboard"
        })
    if verbose_val: print(f"[{context}] Added General Information section to data.")


from selenium.webdriver.common.by import By

def deep_search_in_table_static(driver,
                                table_element,
                                table_index,
                                headers_cache,
                                search_terms_list,
                                keywords_list,
                                section_name,
                                worker_all_data_list,
                                verbose_val,
                                context=""):
    """
    1) Unpack (headers, data_start_idx) from headers_cache[table_index].
    2) Scan each <tr> for search_terms or keywords.
    3) If any matches, emit:
       A) '--- SECTION – SEARCH RESULTS IN TABLE ---'
       B) the cached header row
       C) each matching row (using "blank" for empty cells).
    """
    from selenium.common.exceptions import NoSuchElementException

    try:
        headers, data_start_idx = headers_cache.get(table_index, ([], 0))
        all_rows = table_element.find_elements(By.TAG_NAME, "tr")
        if not all_rows:
            return False

        found = []
        for idx, row in enumerate(all_rows):
            if idx < data_start_idx:
                continue

            cells = row.find_elements(By.TAG_NAME, "td")
            if not cells:
                continue

            # --- build cell values, but only use ALT text if the cell is truly empty ---
            vals = [c.text.strip() for c in cells]
            for j, c in enumerate(cells):
                if not vals[j]:
                    try:
                        alt = c.find_element(By.TAG_NAME, "img").get_attribute("alt") or ""
                        if alt.strip():
                            vals[j] = alt.strip()
                    except NoSuchElementException:
                        pass

            # build searchable strings
            full_text = " ".join(
                (headers[i] if i < len(headers) and headers[i] else f"Col{i}") + " " + vals[i]
                for i in range(len(vals))
            ).lower()
            simple_text = " ".join(v.lower() for v in vals)

            # term match?
            term_hit = any(
                term.lower() in full_text or term.lower() in simple_text
                for term in (search_terms_list or [])
            )

            # keyword match?
            key_hit = False
            if not term_hit and keywords_list:
                key_hit = keyword_match_static(full_text, keywords_list) or keyword_match_static(simple_text, keywords_list)

            if not (term_hit or key_hit):
                continue

            # handle subrow groups
            cls = row.get_attribute("class") or ""
            if "subrow" in cls:
                # group header
                found.append({
                    "Category": vals[0],
                    "Value":    "",
                    "Section":  section_name,
                    "Source":   "DeepSearch - Subrow Group"
                })
                # then its detail rows
                for det in all_rows[idx+1:]:
                    if "subrow" in (det.get_attribute("class") or ""):
                        break
                    dtds = det.find_elements(By.TAG_NAME, "td")
                    if not dtds:
                        continue
                    dvals = [c.text.strip() for c in dtds]
                    # detail values
                    raw = "; ".join(v if v and v != "-" else "blank" for v in dvals[1:])
                    found.append({
                        "Category": f"  ↳ {dvals[0]}",
                        "Value":    raw,
                        "Section":  section_name,
                        "Source":   "DeepSearch - Subrow Detail"
                    })
            else:
                # normal row
                raw = "; ".join(v if v and v != "-" else "blank" for v in vals[1:])
                found.append({
                    "Category": vals[0],
                    "Value":    raw,
                    "Section":  section_name,
                    "Source":   f"DeepSearch - {'Term' if term_hit else 'Keyword'} Match"
                })

        if not found:
            return False

        # emit separator header
        worker_all_data_list.append({
            "Category": f"--- {section_name} - SEARCH RESULTS IN TABLE ---",
            "Value":    "",
            "Section":  section_name,
            "Source":   "Deep Table Search Section Header"
        })
        # emit the cached header row
        emit_cached_header_row(headers, section_name, worker_all_data_list)
        # emit each matched row
        worker_all_data_list.extend(found)
        return True

    except Exception as e:
        if verbose_val:
            import traceback
            print(f"[{context}] deep_search_in_table_static error: {e}\n{traceback.format_exc()}")
        return False

def add_table_to_data_static(headers, data_rows, section_name, source_description,
                             search_terms_list,
                             worker_all_data_list, verbose_val, context=""):
    # --- detect multi-year layout by finding at least two YYYY-YYYY headers ---
    year_pattern = re.compile(r'^\d{4}-\d{4}$')
    # skip the first header (category) and look in the rest
    year_headers = [h for h in headers[1:] if year_pattern.match(str(h).strip())]

    if year_headers:
        if verbose_val:
            print(f"[{context}] Formatting multi-year table into combined rows… Years: {year_headers}")

        # For each row, emit a single record with all year:value pairs
        for row in data_rows:
            if not row or len(row) < 2:
                continue
            category = row[0].strip()
            parts = []
            # headers[1] ↔ row[1], headers[2] ↔ row[2], etc.
            for idx, year in enumerate(year_headers, start=1):
                if idx < len(row):
                    val = row[idx].strip()
                    if val and val != "-":
                        parts.append(f"{year}: {val}")

            if parts:
                worker_all_data_list.append({
                    "Category": category,
                    "Value":    "; ".join(parts),
                    "Section":  section_name,
                    "Source":   f"{source_description} (all years)"
                })
        return

    # --- otherwise fall back to your existing logic ---

    # Label-based (no headers or single blank header)
    is_label_based = (not headers) or (len(headers) == 1 and not headers[0].strip())
    if is_label_based:
        if verbose_val:
            print(f"[{context}] Label-based processing for '{source_description}'")
        for row in data_rows:
            if not row: continue
            label = row[0].strip()
            values = [v.strip() for v in row[1:] if v.strip() and v != "-"]
            worker_all_data_list.append({
                "Category": label,
                "Value":    "; ".join(values),
                "Section":  section_name,
                "Source":   source_description
            })
        return

    # Standard columnar
    if verbose_val:
        print(f"[{context}] Standard columnar for '{source_description}'. Headers: {headers}")
    for row in data_rows:
        if not row: continue
        category = row[0].strip() if row else "Unnamed"
        parts = []
        for col_idx, cell in enumerate(row[1:], start=1):
            val = str(cell).strip()
            if not val or val == "-":
                continue
            # pick the header if it exists and isn’t blank
            hdr = headers[col_idx].strip() if col_idx < len(headers) and headers[col_idx].strip() else f"Col{col_idx}"
            parts.append(f"{hdr}: {val}")

        if parts:
            worker_all_data_list.append({
                "Category": category,
                "Value":    "; ".join(parts),
                "Section":  section_name,
                "Source":   source_description
            })


def process_regular_table_static(driver,
                                 table_element,
                                 index,
                                 headers_cache,
                                 section_name,
                                 search_terms_list,
                                 keywords_list,
                                 worker_all_data_list,
                                 verbose_val,
                                 context=""):
    """
    1) Emit '--- SECTION – ENTIRE TABLE {index+1} CONTENT ---'
    2) Emit one header row via emit_cached_header_row(...)
    3) Emit each data row’s raw values (blank for empty), joined by semicolons.
    """
    try:
        # unpack headers + data_start
        headers, data_start_idx = headers_cache.get(index, ([], 0))

        # collect all rows
        all_rows = table_element.find_elements(By.TAG_NAME, "tr")
        if not all_rows:
            return False

        # slice data rows
        data_rows = []
        for row in all_rows[data_start_idx:]:
            cells = row.find_elements(By.TAG_NAME, "td")
            if cells:
                data_rows.append([c.text.strip() for c in cells])
        if not data_rows:
            return False

        # A) ENTIRE TABLE separator
        worker_all_data_list.append({
            "Category": f"--- {section_name} - ENTIRE TABLE {index+1} CONTENT ---",
            "Value":    "",
            "Section":  section_name,
            "Source":   f"Full Table {index+1} (Header)"
        })

        # B) Single header row
        emit_cached_header_row(headers, section_name, worker_all_data_list)

        # C) Now emit each row’s raw values
        for row_vals in data_rows:
            cat = row_vals[0].strip() or "blank"
            # turn every subsequent cell into 'blank' if empty or '-' else its text
            vals = [v if (v and v != "-") else "blank" for v in row_vals[1:]]
            worker_all_data_list.append({
                "Category": cat,
                "Value":    "; ".join(vals) or "blank",
                "Section":  section_name,
                "Source":   f"Full Table {index+1}"
            })

        return True

    except Exception as e:
        if verbose_val:
            import traceback
            print(f"[{context}] process_regular_table_static error: {e}\n{traceback.format_exc()}")
        return False


def extract_graph_data_static(driver, table_element_or_container, search_terms_list, verbose_val,
                              context=""):  # From your _extract_graph_data
    graph_data_dict = {"title": "Unknown Graph", "images": [], "text_data": {}}
    # This needs the full, detailed logic from your original _extract_graph_data.
    # It was quite comprehensive with title finding, image alt/URL parsing, and text patterns.
    # For brevity, this is a high-level sketch.
    try:
        # --- Title Extraction (from your original) ---
        title_candidates = []
        try:
            title_candidates.append(table_element_or_container.find_element(By.CSS_SELECTOR, "thead th").text.strip())
        except:
            pass
        # ... (all other title finding logic from your original) ...
        if title_candidates: graph_data_dict["title"] = title_candidates[0]

        # --- Image Data (from your original) ---
        images_data_list = []
        graph_images = table_element_or_container.find_elements(By.TAG_NAME, "img")
        for img_element in graph_images:
            img_info = {"alt": (img_element.get_attribute("alt") or ""),
                        "src": (img_element.get_attribute("src") or ""), "extracted_data": {}}
            alt_upper = img_info["alt"].upper()
            if search_terms_list:  # Use passed search_terms_list
                for metric_term in search_terms_list:
                    pattern = rf"{re.escape(metric_term.upper())}.*?(\d+(?:\.\d+)?)%"
                    match_obj = re.search(pattern, alt_upper)
                    if match_obj:
                        img_info["extracted_data"][metric_term] = match_obj.group(1)
            # ... (URL param parsing from your original) ...
            if img_info.get("extracted_data") or img_info["alt"]:
                images_data_list.append(img_info)
        if images_data_list: graph_data_dict["images"] = images_data_list

        # --- Text Data (from your original) ---
        # ... (logic to parse td cells, strong, b, em, i, span for % patterns) ...

    except Exception as e:
        if verbose_val: print(f"[{context}] Error in extract_graph_data_static: {e}")
    return graph_data_dict

def process_graph_table_static(driver,
                               graph_element,
                               index,
                               section_name,
                               search_terms_list,
                               worker_all_data_list,
                               verbose_val,
                               context=""):
    """
    Extracts a graph’s alt-text and any parsed data, then emits rows for:
      • A header marker (--- section – Graph X (Title) ---)
      • Graph Title
      • Image N Description (the raw alt-text)
      • Any numeric metrics found in alt-text or text_data
    """
    # 1) pull the structured graph info
    graph_data = extract_graph_data_static(
        driver,
        graph_element,
        search_terms_list,
        verbose_val,
        context=f"{context} ExtractGraph"
    )

    # 2) skip if nothing meaningful was found
    has_data = (
        graph_data.get("title") != "Unknown Graph"
        or graph_data.get("images")
        or graph_data.get("text_data")
    )
    if not has_data:
        if verbose_val:
            print(f"[{context}] Skipping graph {index+1} in '{section_name}' — no data.")
        return

    source_prefix = f"Graph {index+1} ({graph_data.get('title','Untitled')})"

    # 3) emit a header row
    worker_all_data_list.append({
        "Category": f"--- {section_name} - {source_prefix} ---",
        "Value":    "",
        "Section":  section_name,
        "Source":   source_prefix + " (Header)"
    })

    # 4) emit the title (if known)
    if graph_data.get("title") != "Unknown Graph":
        worker_all_data_list.append({
            "Category": "Graph Title",
            "Value":    graph_data["title"],
            "Section":  section_name,
            "Source":   source_prefix
        })

    # 5) for each <img> we recorded, emit its alt-text and any extracted metrics
    for j, img in enumerate(graph_data.get("images", []), start=1):
        alt = img.get("alt", "")
        if alt:
            worker_all_data_list.append({
                "Category": f"Image {j} Description",
                "Value":    alt,
                "Section":  section_name,
                "Source":   source_prefix
            })
        for metric, val in img.get("extracted_data", {}).items():
            # ensure percentages end with “%”
            text_val = f"{val}%" if not str(val).endswith("%") and str(val).replace(".", "", 1).isdigit() else val
            worker_all_data_list.append({
                "Category": metric,
                "Value":    text_val,
                "Section":  section_name,
                "Source":   f"{source_prefix} - Image {j}"
            })

    # 6) emit any extra text-based metrics
    for label, val in graph_data.get("text_data", {}).items():
        worker_all_data_list.append({
            "Category": label,
            "Value":    val,
            "Section":  section_name,
            "Source":   f"{source_prefix} - Text Data"
        })

    if verbose_val:
        print(f"[{context}] Processed {source_prefix} in '{section_name}'.")

def find_matching_graph_tables_static(driver,
                                     all_graph_elements_on_page,
                                     search_terms_list,
                                     keywords_list,
                                     section_titles_on_page,
                                     verbose_val,
                                     context=""):
    matching_graphs_info = []

    for idx, container in enumerate(all_graph_elements_on_page):
        # figure out which section this graph lives in
        section_name = determine_section_static(
            driver, container, section_titles_on_page, verbose_val,
            f"{context} GraphIdx{idx}"
        )

        # pull together all the alt text for this graph
        alt_text = " ".join(
            img.get_attribute("alt") or ""
            for img in container.find_elements(By.TAG_NAME, "img")
        ).strip().lower()

        # 1) Term-match (if you still want it)
        matched_term = next((
            term for term in search_terms_list
            if term.lower() in alt_text
        ), None)
        if matched_term:
            matching_graphs_info.append({
                'index':     idx,
                'element':   container,
                'match_type': f"term_match_graph ({matched_term})",
                'section':   section_name
            })
            if verbose_val:
                print(f"[{context}] Graph {idx} matched on term '{matched_term}'.")
            continue

        # 2) Percent-graph shortcut (optional)
        if "%" in alt_text:
            matching_graphs_info.append({
                'index':     idx,
                'element':   container,
                'match_type': "percent_graph",
                'section':   section_name
            })
            if verbose_val:
                print(f"[{context}] Graph {idx} matched on '%'.")
            continue

        # 3) **FALLBACK**: any graph with non-empty alt text
        if alt_text:
            matching_graphs_info.append({
                'index':     idx,
                'element':   container,
                'match_type': "any_graph_with_alt",
                'section':   section_name
            })
            if verbose_val:
                print(f"[{context}] Graph {idx} matched by fallback (has alt text).")
            continue

        # otherwise skip graphs with no alt text at all

    if verbose_val:
        print(f"[{context}] Found {len(matching_graphs_info)} matching graphs.")
    return matching_graphs_info


def find_matching_tables_static(driver, all_tables_on_page, search_terms_list, keywords_list, section_titles_on_page,
                                verbose_val, context=""):
    matching_tables_info = []
    if verbose_val: print(
        f"[{context}] Finding matching tables among {len(all_tables_on_page)} tables using terms: {search_terms_list} and keywords: {keywords_list}")

    for i, table_element in enumerate(all_tables_on_page):
        table_specific_context = f"{context} TableIdx{i}"
        # Determine the broad section this table belongs to
        section_name = determine_section_static(driver, table_element, section_titles_on_page, verbose_val,
                                                f"{table_specific_context} SectionDet")

        # Aggregate various text sources from the table for a comprehensive search
        texts_to_check = []
        try:
            # 1. Overall table text (can be noisy but good for broad match)
            texts_to_check.append(table_element.text.lower())
        except Exception as e_text:
            if verbose_val: print(f"[{table_specific_context}] Could not get table.text: {e_text}")

        # 2. Explicitly get <caption> text
        try:
            captions = table_element.find_elements(By.TAG_NAME, "caption")
            for caption_element in captions:  # Iterate as there could be multiple (though unlikely)
                texts_to_check.append(caption_element.text.lower())
        except NoSuchElementException:
            pass  # No caption is fine
        except Exception as e_caption:
            if verbose_val: print(f"[{table_specific_context}] Error getting caption: {e_caption}")

        # 3. Explicitly get text from <thead> <th> elements
        try:
            thead_ths = table_element.find_elements(By.CSS_SELECTOR, "thead th")
            for th_element in thead_ths:
                texts_to_check.append(th_element.text.lower())
        except NoSuchElementException:
            pass
        except Exception as e_thead:
            if verbose_val: print(f"[{table_specific_context}] Error getting thead th: {e_thead}")

        # 4. The section name itself is a crucial piece of context
        if section_name and "Unknown Section" not in section_name:
            texts_to_check.append(section_name.lower())

        # Consolidate all found texts for searching
        combined_searchable_text = " ".join(filter(None, texts_to_check))

        if not combined_searchable_text.strip() and verbose_val:
            print(
                f"[{table_specific_context}] Warning: No searchable text (table.text, caption, thead, valid section) could be extracted for matching.")
            # continue # Optionally skip tables with no text, or let section match try below

        # Now, perform matching based on the aggregated text and section
        matched_this_table = False
        match_reason = ""

        # A. Check for exact search term matches
        if search_terms_list:
            for term in search_terms_list:
                if term.lower() in combined_searchable_text:
                    match_reason = f"exact_term '{term}' in table_text/caption/thead/section"
                    matched_this_table = True
                    break

        # B. If no exact term match, check for keyword matches (all keywords must be present)
        if not matched_this_table and keywords_list:
            if keyword_match_static(combined_searchable_text, keywords_list):
                match_reason = "all_keywords in table_text/caption/thead/section"
                matched_this_table = True

        if matched_this_table:
            # Check if this table (by index i) has already been added to avoid duplicates from different match reasons
            is_already_added = any(info['index'] == i for info in matching_tables_info)
            if not is_already_added:
                matching_tables_info.append({
                    'index': i,
                    'element': table_element,
                    'match_type': match_reason,
                    'section': section_name
                })
                if verbose_val: print(
                    f"[{table_specific_context}] Matched (Reason: {match_reason}). Section: '{section_name}'.")
            elif verbose_val:
                print(
                    f"[{table_specific_context}] Already added based on a previous match rule. Reason: {match_reason}")

    if verbose_val: print(f"[{context}] Found {len(matching_tables_info)} tables matching criteria.")
    return matching_tables_info

def find_matching_graph_tables_static(driver, all_graph_elements_on_page, search_terms_list, keywords_list,
                                      section_titles_on_page, verbose_val,
                                      context=""):  # From your _find_matching_graph_tables
    matching_graphs_info = []
    for idx, container_element in enumerate(all_graph_elements_on_page):
        section_name = determine_section_static(driver, container_element, section_titles_on_page, verbose_val,
                                                f"{context} GraphIdx{idx} SectionDet")
        title_text, alt_text, container_text = "", "", ""
        try:
            try:
                title_text = container_element.find_element(By.CSS_SELECTOR, "thead th, caption").text.strip().lower()
            except:
                pass
            if not title_text:
                try:
                    title_text = container_element.find_element(By.XPATH, ".//svg/title").get_attribute(
                        "textContent").strip().lower()
                except:
                    pass
            for img in container_element.find_elements(By.TAG_NAME, "img"):
                a = (img.get_attribute("alt") or "").strip().lower();
                if a: alt_text += a + " "
            alt_text = alt_text.strip()
            container_text = container_element.text.strip().lower()
        except Exception:
            continue

        matched_term = next((term for term in search_terms_list if
                             term.lower() in title_text or term.lower() in alt_text or term.lower() in container_text),
                            None)
        if matched_term:
            matching_graphs_info.append(
                {'index': idx, 'element': container_element, 'match_type': f"term_match_graph ({matched_term})",
                 'section': section_name})
            if verbose_val: print(f"[{context}] Graph {idx} (Section: {section_name}) matched term '{matched_term}'.")
            continue
        if keywords_list and (keyword_match_static(container_text, keywords_list) or keyword_match_static(title_text,
                                                                                                          keywords_list) or keyword_match_static(
                alt_text, keywords_list)):
            matching_graphs_info.append(
                {'index': idx, 'element': container_element, 'match_type': "keyword_graph", 'section': section_name})
            if verbose_val: print(f"[{context}] Graph {idx} (Section: {section_name}) matched keywords.")
    if verbose_val: print(f"[{context}] Found {len(matching_graphs_info)} graphs initially matching criteria.")
    return matching_graphs_info


def process_matching_elements_static(driver,
                                     worker_all_data_list,
                                     matching_tables_info,
                                     matching_graphs_info,
                                     processing_input,
                                     headers_cache,
                                     verbose_val,
                                     context=""):
    data_added = False
    search_terms = processing_input.get("search_terms", [])
    keywords    = processing_input.get("keywords", [])

    for tbl in matching_tables_info:
        el   = tbl['element']
        idx  = tbl['index']
        sect = tbl['section']
        subc = f"{context} MatchedTable{idx} (Sect:{sect})"

        # deep‐search will still emit its header+rows
        if deep_search_in_table_static(
             driver, el, idx, headers_cache,
             search_terms, keywords,
             sect, worker_all_data_list,
             verbose_val, context=subc+" DeepSearch"
        ):
            data_added = True
        else:
            # regular table now emits only the data rows
            if process_regular_table_static(
                 driver, el, idx, headers_cache,
                 sect, search_terms, keywords,
                 worker_all_data_list, verbose_val,
                 context=subc+" RegularFallback"
            ):
                data_added = True

    for g in matching_graphs_info:
        process_graph_table_static(
            driver,
            g['element'],
            g['index'],
            g['section'],
            processing_input.get("search_terms", []),
            worker_all_data_list,
            verbose_val,
            context=f"{context} MatchedGraph{g['index']}"
        )
        data_added = True

    return data_added


def handle_no_matches_static(driver,
                             worker_all_data_list,
                             all_page_tables,
                             all_page_graph_elements,
                             processing_input,
                             section_titles_on_page,
                             verbose_val,
                             headers_cache,
                             context=""):
    search_terms = processing_input.get("search_terms", [])
    keywords    = processing_input.get("keywords", [])

    # 1) deep‐search fallback
    deep_found = False
    for i, tbl in enumerate(all_page_tables):
        sect = determine_section_static(driver, tbl, section_titles_on_page, verbose_val,
                                        f"{context} FallbackTable{i}")
        if deep_search_in_table_static(
             driver, tbl, i, headers_cache,
             search_terms, keywords, sect,
             worker_all_data_list, verbose_val,
             context=f"{context} FallbackDeepSearch{i}"
        ):
            deep_found = True

    # 2) broad‐keyword fallback if deep‐search found nothing
    if not deep_found and keywords:
        kw_tables = []
        for i, tbl in enumerate(all_page_tables):
            sect = determine_section_static(driver, tbl, section_titles_on_page, verbose_val,
                                            f"{context} FallbackKeywordTable{i}")
            if keyword_match_static(tbl.text.lower(), keywords):
                kw_tables.append((i, tbl, sect))

        for idx, tbl, sect in kw_tables:
            # A) separator
            worker_all_data_list.append({
                "Category": f"--- {sect} - SEARCH RESULTS IN TABLE ---",
                "Value":    "",
                "Section":  sect,
                "Source":   "Deep Table Search Section Header"
            })
            # B) emit data rows only
            process_regular_table_static(
                driver, tbl, idx, headers_cache,
                sect, [], [],  # no filtering at this point
                worker_all_data_list,
                verbose_val,
                context=f"{context} KeywordFallback{idx}"
            )


def emit_cached_header_row(headers, section_name, worker_all_data_list):
    """
    Emits exactly one header row:
      - first header → Category
      - rest → semicolon-joined Value
    """
    first = headers[0].strip() if headers and headers[0].strip() else "blank"
    rest  = [h for h in headers[1:] if h.strip()]
    worker_all_data_list.append({
        "Category": first,
        "Value":    "; ".join(rest) or "blank",
        "Section":  section_name,
        "Source":   "Deep Table Search – Table Headers"
    })


def check_general_info_for_search_static(worker_institution_info_dict, search_terms_list,
                                         keywords_list):  # From your _check_general_info_for_search
    # Returns a list of strings describing matches, or empty list if no match.
    found_matches_strings = []
    if not worker_institution_info_dict: return found_matches_strings

    for key, value in worker_institution_info_dict.items():
        key_lower = key.lower()
        value_lower = str(value).lower()
        combined_text = f"{key_lower} {value_lower}"

        term_matched = False
        if search_terms_list:
            for term in search_terms_list:
                if term.lower() in combined_text:
                    found_matches_strings.append(f"'{key}: {value}' (Matched by term: '{term}')")
                    term_matched = True
                    break
        if not term_matched and keywords_list:
            if keyword_match_static(combined_text, keywords_list):
                found_matches_strings.append(f"'{key}: {value}' (Matched by keywords)")
    return found_matches_strings


def find_and_process_tables_graphs_static(driver,
                                          worker_all_data_list,
                                          worker_institution_info_dict,
                                          processing_input,
                                          section_titles_on_page,
                                          verbose_val,
                                          context=""):
    # 1) Grab _all_ tables, then split them into “real” tables vs. graph‐tables:
    all_tables = driver.find_elements(By.TAG_NAME, "table")
    graph_tables = [t for t in all_tables if "graphtabs" in (t.get_attribute("class") or "")]
    data_tables  = [t for t in all_tables if t not in graph_tables]

    # 2) Cache headers on the data_tables only
    headers_cache = {}
    for i, tbl in enumerate(data_tables):
        rows = tbl.find_elements(By.TAG_NAME, "tr")
        hdrs, start = get_table_headers_and_data_start_idx_static(tbl, rows, False, f"{context} Cache{i}")
        headers_cache[i] = (hdrs, start)

    # 3) Find matches on data_tables _only_
    matching_tables = find_matching_tables_static(
        driver,
        data_tables,
        processing_input.get("search_terms", []),
        processing_input.get("keywords", []),
        section_titles_on_page,
        verbose_val,
        f"{context} TableMatch"
    )

    # 4) Find matches on graph_tables _only_
    matching_graphs = find_matching_graph_tables_static(
        driver,
        graph_tables,
        processing_input.get("search_terms", []),
        processing_input.get("keywords", []),
        section_titles_on_page,
        verbose_val,
        f"{context} GraphMatch"
    )

    # 5) Process them separately
    data_added = False
    if matching_tables:
        data_added |= process_matching_elements_static(
            driver,
            worker_all_data_list,
            matching_tables,
            [],              # no graphs here
            processing_input,
            headers_cache,
            verbose_val,
            f"{context} ProcTables"
        )

    if matching_graphs:
        data_added |= process_matching_elements_static(
            driver,
            worker_all_data_list,
            [],              # no tables here
            matching_graphs,
            processing_input,
            headers_cache,
            verbose_val,
            f"{context} ProcGraphs"
        )

    # 6) Your existing fallback on data_tables only...
    if not data_added:
        handle_no_matches_static(
            driver,
            worker_all_data_list,
            data_tables,
            matching_graphs,  # graphs go to graph‐fallback if you have one
            processing_input,
            section_titles_on_page,
            verbose_val,
            headers_cache,
            f"{context} Fallback"
        )


def process_institution_page_static(driver, worker_all_data_list, worker_institution_info_dict,
                                    processing_input, verbose_val,
                                    context=""):  # Replicates your _process_institution_page
    try:
        current_institution_name = processing_input.get("current_institution_name", "UnknownInst")
        if verbose_val: print(f"[{context}] Starting to process page content for: {current_institution_name}")

        section_header_elements = driver.find_elements(By.CSS_SELECTOR, ".tabtitles")
        all_page_section_titles = [h.text.strip() for h in section_header_elements if h.text.strip()]

        # Replicate VALID_START_CATEGORY logic from your _process_institution_page
        valid_sections_started = False
        VALID_START_CATEGORY = "GENERAL INFORMATION"  # As in your original
        section_titles_to_use = []
        for title in all_page_section_titles:
            if not title: continue
            if title.upper() == VALID_START_CATEGORY:  # Match flexibly
                valid_sections_started = True
            if valid_sections_started:
                section_titles_to_use.append(title)

        if not section_titles_to_use and all_page_section_titles:  # Fallback if VALID_START_CATEGORY not found
            if verbose_val: print(
                f"[{context}] VALID_START_CATEGORY ('{VALID_START_CATEGORY}') not found. Using all found section titles.")
            section_titles_to_use = all_page_section_titles
        elif not section_titles_to_use:
            if verbose_val: print(f"[{context}] No section titles found on page at all.")
            # section_titles_to_use remains empty, subsequent functions should handle this

        if verbose_val:
            print(f"[{context}] Available data categories (section titles) for processing: {section_titles_to_use}")

        # Call the static equivalent of your _find_and_process_tables
        find_and_process_tables_graphs_static(driver, worker_all_data_list, worker_institution_info_dict,
                                              processing_input,
                                              section_titles_to_use,  # Pass the filtered/actual list
                                              verbose_val, f"{context} PageContentExtraction")

        if verbose_val: print(f"[{context}] Finished processing page content for {current_institution_name}.")

    except Exception as e:
        if verbose_val:
            import traceback
            print(
                f"[{context}] CRITICAL Error in process_institution_page_static for {processing_input.get('current_institution_name', 'UnknownInst')}: {e}\n{traceback.format_exc()}")


# --- WORKER FUNCTION (Top-Level for ProcessPoolExecutor) ---
def scrape_college_data_worker(driver_path_val, base_url_val, wait_time_val, verbose_val,
                               target_info, user_overall_input_val):
    """
    Worker that scrapes one institution page and returns (institution_name, DataFrame).
    Each worker self-installs a matching chromedriver via webdriver_manager.
    """
    worker_driver = None
    institution_name = target_info['name']
    institution_href = target_info['href']
    worker_all_data = []
    worker_institution_info_dict = {}
    context = f"Worker {institution_name}"

    if verbose_val:
        print(f"[{context}] Starting...")

    try:
        # 1) build Chrome options
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])

        # 2) install & launch matching chromedriver
        driver_path = ChromeDriverManager().install()
        service     = Service(driver_path)
        worker_driver = webdriver.Chrome(service=service, options=chrome_options)
        if verbose_val:
            print(f"[{context}] WebDriver initialized at {driver_path}")

        # 3) navigate to institution page
        if not institution_href.startswith("http"):
            institution_href = urllib.parse.urljoin(base_url_val, institution_href)
        worker_driver.get(institution_href)
        if verbose_val:
            print(f"[{context}] Navigated to: {institution_href}")

        # 4) wait for page load (reuse your static helper)
        wait_for_page_load_static(worker_driver, wait_time_val, verbose_val, context)

        # 5) extract general info
        worker_institution_info_dict = extract_general_info_static(worker_driver, verbose_val, context)

        # 6) conditionally add general info to worker_all_data
        add_general_info_to_data_static(
            worker_all_data,
            worker_institution_info_dict,
            user_overall_input_val.get("search_terms"),
            verbose_val,
            context
        )

        # 7) expand all sections (so tables/graphs load)
        expand_all_sections_static(worker_driver, verbose_val, context)

        # 8) process main page content
        processing_input = user_overall_input_val.copy()
        processing_input["current_institution_name"] = institution_name
        process_institution_page_static(
            worker_driver,
            worker_all_data,
            worker_institution_info_dict,
            processing_input,
            verbose_val,
            context
        )

        # 9) package up result
        if worker_all_data:
            df = pd.DataFrame(worker_all_data)
            if verbose_val:
                print(f"[{context}] Collected {len(df)} records.")
            return institution_name, df
        else:
            if verbose_val:
                print(f"[{context}] No data found.")
            return institution_name, pd.DataFrame()

    except Exception as e:
        print(f"[{context}] CRITICAL ERROR: {e}\n{traceback.format_exc()}")
        return institution_name, pd.DataFrame()

    finally:
        if worker_driver:
            worker_driver.quit()
        if verbose_val:
            print(f"[{context}] Driver quit, worker finished.")


class CollegeNavigatorScraper:
    def __init__(self, driver_path="C:/chromedriver/chromedriver.exe", verbose=False):  # User's default path
        self.driver_path = driver_path
        self.driver = None
        self.base_url = "https://nces.ed.gov/collegenavigator/"
        self.wait_time = 20  # Using a slightly higher wait time
        self.verbose = verbose

    def _setup_driver(self):  # For main thread's search/selection
        try:
            chrome_options = Options()
            chrome_options.add_argument("--incognito")

            # ── auto-download (or reuse) the matching Chromedriver ──
            driver_path = ChromeDriverManager().install()
            service = Service(driver_path)

            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            if self.verbose:
                print("Main browser launched successfully for search/selection.")
        except Exception as e:
            print(f"Failed to set up the main browser: {e}")
            raise

    # Using user's provided _normalize_search_term and _extract_keywords
    def _normalize_search_term(self, term):  # User's original
        term = term.lower()
        replacements = {
            "graduation rate": "grad rate", "graduation": "grad",
            "6-year": "6 year", "six-year": "6-year", "six year": "6-year",
        }
        normalized_terms = {term}
        temp_term = term  # Original script had this but it wasn't really used effectively in loop
        # A better way to use replacements on the original term:
        for old, new in replacements.items():
            if old in term:  # Check in original term
                normalized_terms.add(term.replace(old, new))

        # Original specific handling for 6-year variations
        if "6 year" in term and "6-year" not in term: normalized_terms.add(term.replace("6 year", "6-year"))
        if "6-year" in term and "6 year" not in term: normalized_terms.add(term.replace("6-year", "6 year"))

        if self.verbose: print(f"[DEBUG] Normalized '{term}' to {list(normalized_terms)}")
        return list(normalized_terms)

    def _extract_keywords(self, search_terms_list):  # User's original
        all_keywords = set()
        stop_words = {"of", "the", "and", "in", "for", "to", "a", "is", "with", "on"}
        for term_phrase in search_terms_list:
            words_in_phrase = term_phrase.split()
            meaningful_words = [word for word in words_in_phrase if len(word) > 2 and word not in stop_words]
            all_keywords.update(meaningful_words)
        if self.verbose: print(f"[DEBUG] Extracted keywords: {list(all_keywords)} from {search_terms_list}")
        return list(all_keywords)

    def _get_user_input(self):
        names_input = input("Enter names of Colleges or Universities (comma separated): ")
        names = [name.strip() for name in names_input.split(",") if name.strip()]
        search_term_input = input(
            "What specific information are you looking for? (e.g., 'Average net price', 'Retention rates'): ")

        # These are now instance methods, results passed to worker
        search_terms = self._normalize_search_term(search_term_input)
        keywords = self._extract_keywords(search_terms)

        return {
            "names": names,
            "search_term_original": search_term_input,  # For filename primarily
            "search_terms": search_terms,  # Normalized list
            "keywords": keywords,
            "clean_search_filename_part": clean_filename_static(search_term_input)
        }

    def _search_institution(self, institution_name):
        try:
            self.driver.get(self.base_url)

            # wait for the page to load the search form
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located((By.CLASS_NAME, "instruct"))
            )

            # ——— check the “4-year” box ———
            try:
                ck4 = self.driver.find_element(
                    By.ID,
                    "ctl00_cphCollegeNavBody_ucSearchMain_chkLevelFourYear"
                )
                if not ck4.is_selected():
                    ck4.click()
                    if self.verbose:
                        print("4-year checkbox selected")
            except NoSuchElementException:
                if self.verbose:
                    print("4-year checkbox not found; continuing without it")

            # ——— now perform the search ———
            input_el = self.driver.find_element(By.CLASS_NAME, "instruct")
            input_el.clear()
            input_el.send_keys(institution_name)

            # click the “Show Results” button rather than ENTER, so our filter takes effect
            btn = self.driver.find_element(
                By.ID,
                "ctl00_cphCollegeNavBody_ucSearchMain_btnSearch"
            )
            btn.click()

            # wait for results table
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located(
                    (By.ID, "ctl00_cphCollegeNavBody_ucResultsMain_tblResults")
                )
            )
        except TimeoutException:
            print(f"Timed out waiting for search results for '{institution_name}'.")
        except Exception as e:
            print(f"Error searching for institution '{institution_name}': {e}")

    def _select_institution_from_results(self):  # Uses self.driver
        selected_targets_info = []
        try:
            WebDriverWait(self.driver, self.wait_time).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".resultsTable"))
            )
            results_web_elements = self.driver.find_elements(By.CSS_SELECTOR, ".resultsTable tr td a[href*='id=']")
            num_results = len(results_web_elements)

            if num_results == 0: print("No institutions found from search."); return []
            if num_results == 1:
                first_result = results_web_elements[0]
                name = first_result.text.strip();
                href = first_result.get_attribute('href')
                print(f"Single result found and automatically selected: {name}")
                selected_targets_info.append({'name': name, 'href': href})
                return selected_targets_info
            else:  # Multiple results, call user's _handle_multiple_results
                return self._handle_multiple_results(results_web_elements)
        except TimeoutException:
            print("Timed out waiting for results table in _select_institution_from_results."); return []
        except Exception as e:
            print(f"Error in _select_institution_from_results: {e}"); return []

    def _handle_multiple_results(self, results_web_elements):  # User's provided version
        num_results = len(results_web_elements)
        print(f"Found {num_results} search results.")
        print("Your entry pulled multiple results. Please select one or more of the following:")
        for index, result_element in enumerate(results_web_elements, start=1):
            print(f"{index}. {result_element.text.strip()}")
        chosen_targets_info = []
        while True:
            try:
                selection_input_str = input(
                    f"Enter the numbers of the institutions you want to select (1-{num_results}, separated by commas, or 0 to skip current query): ")
                if selection_input_str.strip() == '0': print("Skipping selection for this query."); return []
                selected_indices_str = [s.strip() for s in selection_input_str.split(',') if s.strip()]
                if not selected_indices_str: print("No selection made. Please enter numbers or 0 to skip."); continue

                valid_selections_made_this_attempt = False
                temp_chosen_targets_this_attempt = []
                processed_indices_this_attempt = set()
                for index_str in selected_indices_str:
                    user_selection = int(index_str)
                    if 1 <= user_selection <= num_results:
                        if user_selection in processed_indices_this_attempt:
                            if self.verbose: print(
                                f"Note: Number {user_selection} was entered multiple times, processing once.")
                            continue
                        selected_element = results_web_elements[user_selection - 1]
                        name = selected_element.text.strip();
                        href = selected_element.get_attribute('href')
                        print(f"Added for processing: {name}")
                        temp_chosen_targets_this_attempt.append({'name': name, 'href': href})
                        processed_indices_this_attempt.add(user_selection)
                        valid_selections_made_this_attempt = True
                    else:
                        print(f"Invalid selection: '{user_selection}'. Number must be between 1 and {num_results}.")
                if valid_selections_made_this_attempt:
                    chosen_targets_info = temp_chosen_targets_this_attempt; break
                elif not temp_chosen_targets_this_attempt and selected_indices_str:
                    print("All numbers entered were invalid. Please try again or enter 0 to skip.")
            except ValueError:
                print("Invalid input. Please enter numbers separated by commas (e.g., 1, 3, 5) or 0 to skip.")
            except Exception as e:
                print(f"An unexpected error occurred during selection: {e}"); return []
        return chosen_targets_info

    def _save_multiple_excel_data(self, dfs_dict, user_overall_input):
        if not dfs_dict:
            print("No data collected for any institution to save.")
            return

        # Filter out empty or None DataFrames
        dfs_to_save = {name: df for name, df in dfs_dict.items() if df is not None and not df.empty}
        if not dfs_to_save:
            print("No data to save to Excel after filtering empty/None datasets.")
            return

        filename_suffix = user_overall_input.get("clean_search_filename_part", "data")
        filename = f"college_data_{filename_suffix}.xlsx"

        try:
            with pd.ExcelWriter(filename, engine='xlsxwriter') as writer:
                for inst_name, df in dfs_to_save.items():
                    # 1) Determine sheet name and write the raw DataFrame
                    sheet_name = clean_filename_static(inst_name)[:31]
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    workbook = writer.book
                    worksheet = writer.sheets[sheet_name]

                    # ─── 2) Create/bold the header row ───
                    header_fmt = workbook.add_format({
                        'bold': True,
                        'bg_color': '#D9E1F2',
                        'font_size': 12,
                        'align': 'left',
                        'valign': 'vcenter'
                    })
                    # Apply the format to the entire first row
                    worksheet.set_row(0, None, header_fmt)

                    # ─── 3) Freeze panes and add autofilter ───
                    worksheet.freeze_panes(1, 0)
                    worksheet.autofilter(0, 0, df.shape[0], df.shape[1] - 1)

                    # ─── 4) Adjust column widths (and wrap the Value column) ───
                    for col_idx, column in enumerate(df.columns):
                        if column.lower() == 'value':
                            # Wrap text in the Value column
                            wrap_fmt = workbook.add_format({'text_wrap': True, 'valign': 'top'})
                            max_len = df[column].astype(str).map(len).max()
                            width = min(max(max_len, len(column)) + 4, 60)
                            worksheet.set_column(col_idx, col_idx, width, wrap_fmt)
                        else:
                            # Auto‐fit other columns
                            try:
                                max_len = df[column].astype(str).map(len).max()
                            except ValueError:
                                max_len = 0
                            width = min(max(max_len, len(column)) + 4, 60)
                            worksheet.set_column(col_idx, col_idx, width)

                    # ─── 5) Highlight section separator rows ───
                    sep_fmt = workbook.add_format({
                        'bold': True,
                        'font_size': 11,
                        'bg_color': '#E7E6E6'
                    })
                    for row_idx, row_data in df.iterrows():
                        cat = str(row_data.get('Category', ''))
                        if cat.startswith('---') or cat.upper() == 'GENERAL INFORMATION':
                            # +1 because Excel rows include the header at row 0
                            worksheet.set_row(row_idx + 1, None, sep_fmt)

            print(f"\nAll data saved to {filename}.")
        except Exception as e:
            print(f"Error saving Excel file '{filename}': {e}")
            import traceback;
            traceback.print_exc()

    def _cleanup(self):
        if self.driver:
            if self.verbose: print("Main driver cleanup initiated...")
            try:
                # input("\nOriginal script had: Press Enter to close the browser...") # Kept as comment
                pass
            finally:
                self.driver.quit()
                self.driver = None
                if self.verbose: print("Main browser closed.")

    def run(self):
        all_dfs_for_excel = {}
        institutions_to_scrape_args = []
        try:
            user_overall_input = self._get_user_input()
            self._setup_driver()
            if not self.driver: print("Main driver setup failed. Exiting."); return

            for initial_search_query in user_overall_input["names"]:
                print(f"\n====== Searching based on user query: '{initial_search_query}' ======")
                try:
                    self._search_institution(initial_search_query)
                    selected_targets = self._select_institution_from_results()
                    if not selected_targets: print(f"No institutions for query '{initial_search_query}'."); continue
                    for target_info in selected_targets:
                        institutions_to_scrape_args.append(
                            (self.driver_path, self.base_url, self.wait_time, self.verbose,
                             target_info, user_overall_input.copy())
                        )
                except Exception as e_search:
                    print(f"Error during search/selection for '{initial_search_query}': {e_search}")

            if self.driver: self.driver.quit(); self.driver = None;
            if self.verbose and not self.driver: print("Main browser for search/selection closed.")

            if not institutions_to_scrape_args: print("No institutions selected for detailed scraping."); return

            num_targets = len(institutions_to_scrape_args)
            max_allowed_workers = 4  # Tune this based on your system and website tolerance
            num_workers = min(num_targets, max_allowed_workers)
            if num_workers == 0 and num_targets > 0: num_workers = 1

            if self.verbose: print(
                f"\nStarting parallel processing for {num_targets} target(s) with {num_workers} worker(s)...")

            if num_workers > 0:
                with ProcessPoolExecutor(max_workers=num_workers) as executor:
                    futures = [executor.submit(scrape_college_data_worker, *args) for args in
                               institutions_to_scrape_args]
                    for future in futures:
                        try:
                            name, df = future.result()
                            if name:
                                all_dfs_for_excel[name] = df
                                if self.verbose:
                                    status = "successfully" if df is not None and not df.empty else "with no data or an error"
                                    df_len = len(df) if df is not None else 0
                                    print(f"Data for '{name}' processed {status}. Records: {df_len}")
                        except Exception as exc_future:
                            print(
                                f'A worker process for a target generated an exception during result retrieval: {exc_future}')
                if self.verbose: print("\nParallel processing finished.")
            else:
                if self.verbose: print("No targets to process in parallel.")

            self._save_multiple_excel_data(all_dfs_for_excel, user_overall_input)

        except Exception as e:
            print(f"A critical error occurred in run: {e}")
            import traceback;
            traceback.print_exc()
        finally:
            self._cleanup()  # Ensure main driver is attempted to be closed if anything went wrong


if __name__ == "__main__":
    scraper = CollegeNavigatorScraper(verbose=True)
    scraper.run()