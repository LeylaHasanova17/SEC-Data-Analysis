import pandas as pd
import requests
import time
import os

def categorize_assets(val):
    """
    Categorizes companies based on their total asset value.
    Note: Values < 1000 USD usually indicate inactive/shell companies.
    """
    try:
        val = float(val)
        if val < 1000:
            return "Shell/Inactive"
        elif val < 1e8:
            return "Micro"
        elif val < 5e8:
            return "Small"
        elif val < 2e9:
            return "Mid-Cap"
        elif val < 1e10:
            return "Mid-Plus"
        elif val < 1e11:
            return "Large"
        else:
            return "Mega"
    except (ValueError, TypeError):
        return "N/A"

def run_sec_pipeline(input_path, output_path="SEC_Results.csv"):
    """
    Main function to fetch asset data from SEC EDGAR API.
    """
    # Resume progress if output file already exists
    if os.path.exists(output_path):
        print(f"Resuming from existing file: {output_path}")
        df = pd.read_csv(output_path)
    else:
        print("Starting a new data extraction run...")
        df = pd.read_csv(input_path)
        # Normalize column names for consistency
        df.columns = df.columns.str.strip().str.lower()

        if "cik" not in df.columns:
            print("Error: 'cik' column not found in input file.")
            return None

        # Prepare CIKs and new columns
        df["cik_padded"] = df["cik"].astype(str).str.split(".").str[0].str.zfill(10)
        df["latest_assets"] = pd.NA
        df["asset_category"] = pd.NA
        df["latest_filing_year"] = pd.NA

    # SEC requires a descriptive User-Agent header
    # Replace the email with your academic/professional email
    headers = {
        "User-Agent": "Research Script (contact: your-email@example.com)"
    }

    # Filter for companies that haven't been processed yet
    remaining = df[df["latest_assets"].isna()].index.tolist()
    total = len(remaining)
    print(f"Total companies to process: {total}")

    for i, idx in enumerate(remaining, 1):
        cik = df.at[idx, "cik_padded"]
        url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

        try:
            # Respect SEC rate limits (10 requests per second)
            response = requests.get(url, headers=headers, timeout=15)
            time.sleep(0.15) 

            if response.status_code == 200:
                facts = response.json().get("facts", {}).get("us-gaap", {})
                
                # Check for Assets data in USD
                if "Assets" in facts and "USD" in facts["Assets"].get("units", {}):
                    # Filter for 10-K filings only to get annual data
                    data = [x for x in facts["Assets"]["units"]["USD"] 
                            if x.get("form") == "10-K" and "fy" in x]
                    
                    if data:
                        # Sort by fiscal year and pick the most recent one
                        latest = sorted(data, key=lambda x: x["fy"])[-1]
                        val = latest["val"]
                        
                        df.at[idx, "latest_assets"] = val
                        df.at[idx, "asset_category"] = categorize_assets(val)
                        df.at[idx, "latest_filing_year"] = latest["fy"]
                        print(f"[{i}/{total}] CIK {cik}: Data successfully retrieved.")
                    else:
                        df.at[idx, "latest_assets"] = "N/A"
                        print(f"[{i}/{total}] CIK {cik}: No 10-K data found.")
                else:
                    df.at[idx, "latest_assets"] = "N/A"
            else:
                df.at[idx, "latest_assets"] = "N/A"
                print(f"[{i}/{total}] CIK {cik}: API Request failed (Status {response.status_code}).")

        except Exception as e:
            print(f"Error processing CIK {cik}: {e}")
            continue

        # Save progress every 25 rows to prevent data loss
        if i % 25 == 0:
            df.to_csv(output_path, index=False)
            print(f"--- Progress saved at {i} companies ---")

    # Final save
    df.to_csv(output_path, index=False)
    print(f"Pipeline complete. Results saved to: {output_path}")
    return df

if __name__ == "__main__":
    # Update these paths as needed for your local environment
    INPUT_FILE = "Publicly_Trade_Companies_SEC.csv" 
    run_sec_pipeline(INPUT_FILE)
