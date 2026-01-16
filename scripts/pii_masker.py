#!/usr/bin/env python3
"""
PII Masker - Automatically detect and mask sensitive/PII data in CSV files.

This script scans CSV files for common PII patterns and replaces them with
masked placeholders while generating a reversible mapping file.

Usage:
    python pii_masker.py input.csv [--output output_masked.csv] [--mapping mapping.json]
    
Example:
    python pii_masker.py sample_2_tickets.csv --output sample_2_tickets_masked.csv
"""

import re
import csv
import json
import argparse
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple, Set
from dataclasses import dataclass, field


@dataclass
class PIIPattern:
    """Defines a PII pattern for detection."""
    name: str
    pattern: str
    mask_prefix: str
    description: str
    flags: int = re.IGNORECASE


@dataclass 
class MaskingResult:
    """Result of masking operation."""
    masked_text: str
    mappings: Dict[str, Dict[str, str]]
    stats: Dict[str, int]


class PIIMasker:
    """
    Detects and masks Personally Identifiable Information (PII) in text.
    """
    
    # Common PII patterns
    PII_PATTERNS: List[PIIPattern] = [
        PIIPattern(
            name="email",
            pattern=r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            mask_prefix="EMAIL_MASKED",
            description="Email addresses"
        ),
        PIIPattern(
            name="phone_us",
            pattern=r'\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}\b',
            mask_prefix="PHONE_MASKED",
            description="US phone numbers"
        ),
        PIIPattern(
            name="phone_intl",
            pattern=r'\b\+\d{1,3}[-.\s]?\d{1,4}[-.\s]?\d{1,4}[-.\s]?\d{1,9}\b',
            mask_prefix="PHONE_INTL_MASKED",
            description="International phone numbers"
        ),
        PIIPattern(
            name="ssn",
            pattern=r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b',
            mask_prefix="SSN_MASKED",
            description="Social Security Numbers",
            flags=0
        ),
        PIIPattern(
            name="credit_card",
            pattern=r'\b(?:\d{4}[-\s]?){3}\d{4}\b',
            mask_prefix="CC_MASKED",
            description="Credit card numbers",
            flags=0
        ),
        PIIPattern(
            name="ip_address",
            pattern=r'\b(?:\d{1,3}\.){3}\d{1,3}\b',
            mask_prefix="IP_MASKED",
            description="IP addresses",
            flags=0
        ),
        PIIPattern(
            name="uuid",
            pattern=r'\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b',
            mask_prefix="UUID_MASKED",
            description="UUIDs/GUIDs (MOI, etc.)"
        ),
        PIIPattern(
            name="student_id",
            pattern=r'\bStudent\s*ID[:\s]*(\d{6,12})\b',
            mask_prefix="STUDENT_ID_MASKED",
            description="Student IDs"
        ),
        PIIPattern(
            name="profile_url",
            pattern=r'https?://[^\s<>"]+/(?:profile|user|account|show_profile)/\d+[^\s<>"]*',
            mask_prefix="PROFILE_URL_MASKED",
            description="Profile URLs with IDs"
        ),
    ]
    
    # Common name patterns (first names that often appear in greetings)
    GREETING_PATTERNS = [
        r'\bHi\s+([A-Z][a-z]+)',
        r'\bHello\s+([A-Z][a-z]+)',
        r'\bDear\s+([A-Z][a-z]+)',
        r'\bThanks?,?\s+([A-Z][a-z]+)',
        r'\bRegards,?\s+([A-Z][a-z]+)',
    ]
    
    # Known system/staff emails to preserve or mark differently
    SYSTEM_EMAIL_DOMAINS = [
        'agilent.com',
        'ilabsolutions.com',
    ]
    
    def __init__(self, 
                 mask_staff_emails: bool = False,
                 mask_names_in_greetings: bool = True,
                 custom_patterns: List[PIIPattern] = None):
        """
        Initialize the PII Masker.
        
        Args:
            mask_staff_emails: Whether to mask internal/staff emails
            mask_names_in_greetings: Whether to mask names in greetings like "Hi John"
            custom_patterns: Additional custom patterns to detect
        """
        self.mask_staff_emails = mask_staff_emails
        self.mask_names_in_greetings = mask_names_in_greetings
        self.patterns = self.PII_PATTERNS.copy()
        if custom_patterns:
            self.patterns.extend(custom_patterns)
        
        # Tracking
        self.mappings: Dict[str, Dict[str, str]] = {}
        self.counters: Dict[str, int] = {}
        self.seen_values: Dict[str, str] = {}  # For consistent masking
        
    def reset(self):
        """Reset all tracking state."""
        self.mappings = {}
        self.counters = {}
        self.seen_values = {}
        
    def _get_mask_id(self, category: str, value: str) -> str:
        """
        Get or create a consistent mask ID for a value.
        Uses value hash for consistency across the document.
        """
        # Normalize the value
        normalized = value.lower().strip()
        key = f"{category}:{normalized}"
        
        if key in self.seen_values:
            return self.seen_values[key]
        
        # Create new mask ID
        if category not in self.counters:
            self.counters[category] = 0
        self.counters[category] += 1
        
        mask_id = f"[{category}_{self.counters[category]}]"
        self.seen_values[key] = mask_id
        
        # Store mapping
        if category not in self.mappings:
            self.mappings[category] = {}
        self.mappings[category][mask_id.strip('[]')] = value
        
        return mask_id
    
    def _is_system_email(self, email: str) -> bool:
        """Check if email belongs to a system/staff domain."""
        return any(domain in email.lower() for domain in self.SYSTEM_EMAIL_DOMAINS)
    
    def mask_text(self, text: str) -> str:
        """
        Mask all detected PII in the given text.
        
        Args:
            text: Input text to mask
            
        Returns:
            Text with PII replaced by mask placeholders
        """
        if not text or not isinstance(text, str):
            return text
            
        masked = text
        
        # Apply each pattern
        for pii_pattern in self.patterns:
            regex = re.compile(pii_pattern.pattern, pii_pattern.flags)
            
            # Find all matches first to avoid issues with overlapping replacements
            matches = list(regex.finditer(masked))
            
            # Replace from end to start to preserve positions
            for match in reversed(matches):
                original_value = match.group(0)
                
                # Special handling for emails
                if pii_pattern.name == "email":
                    if self._is_system_email(original_value):
                        if not self.mask_staff_emails:
                            continue
                        category = "EMAIL_SYSTEM_MASKED"
                    else:
                        category = pii_pattern.mask_prefix
                else:
                    category = pii_pattern.mask_prefix
                
                mask_id = self._get_mask_id(category, original_value)
                masked = masked[:match.start()] + mask_id + masked[match.end():]
        
        # Mask names in greetings
        if self.mask_names_in_greetings:
            for greeting_pattern in self.GREETING_PATTERNS:
                regex = re.compile(greeting_pattern)
                matches = list(regex.finditer(masked))
                for match in reversed(matches):
                    if match.group(1):
                        name = match.group(1)
                        # Don't mask if it looks like a masked value already
                        if not name.startswith('[') and len(name) > 2:
                            mask_id = self._get_mask_id("NAME_MASKED", name)
                            full_match = match.group(0)
                            replacement = full_match.replace(name, mask_id)
                            masked = masked[:match.start()] + replacement + masked[match.end():]
        
        return masked
    
    def mask_csv(self, input_path: str, output_path: str = None, 
                 mapping_path: str = None) -> MaskingResult:
        """
        Mask PII in a CSV file.
        
        Args:
            input_path: Path to input CSV file
            output_path: Path for masked output (default: input_masked.csv)
            mapping_path: Path for mapping JSON (default: pii_mapping_<filename>.json)
            
        Returns:
            MaskingResult with masked file info and statistics
        """
        self.reset()
        
        input_path = Path(input_path)
        if not output_path:
            output_path = input_path.parent / f"{input_path.stem}_masked{input_path.suffix}"
        if not mapping_path:
            mapping_path = input_path.parent / f"pii_mapping_{input_path.stem}.json"
            
        output_path = Path(output_path)
        mapping_path = Path(mapping_path)
        
        # Read and process CSV
        rows_processed = 0
        masked_rows = []
        
        with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
            # Detect delimiter
            sample = f.read(4096)
            f.seek(0)
            
            try:
                dialect = csv.Sniffer().sniff(sample)
            except csv.Error:
                dialect = csv.excel
            
            reader = csv.reader(f, dialect)
            
            for row in reader:
                masked_row = []
                for cell in row:
                    masked_cell = self.mask_text(cell)
                    masked_row.append(masked_cell)
                masked_rows.append(masked_row)
                rows_processed += 1
        
        # Write masked output
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(masked_rows)
        
        # Calculate statistics
        stats = {
            "rows_processed": rows_processed,
            "total_pii_found": sum(len(v) for v in self.mappings.values()),
        }
        for category, items in self.mappings.items():
            stats[f"{category.lower()}_count"] = len(items)
        
        # Save mapping
        mapping_data = {
            "metadata": {
                "source_file": str(input_path.name),
                "masked_file": str(output_path.name),
                "created_at": datetime.now().isoformat(),
                "description": "PII mapping for reversibility - CONFIDENTIAL",
                "statistics": stats
            },
            **self.mappings
        }
        
        with open(mapping_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*60}")
        print(f"PII Masking Complete")
        print(f"{'='*60}")
        print(f"Input file:    {input_path}")
        print(f"Output file:   {output_path}")
        print(f"Mapping file:  {mapping_path}")
        print(f"\nStatistics:")
        print(f"  Rows processed:  {stats['rows_processed']}")
        print(f"  Total PII found: {stats['total_pii_found']}")
        for category, items in self.mappings.items():
            print(f"  {category}: {len(items)}")
        print(f"{'='*60}\n")
        
        return MaskingResult(
            masked_text=str(output_path),
            mappings=self.mappings,
            stats=stats
        )


class PIIUnmasker:
    """
    Reverses PII masking using a mapping file.
    """
    
    def __init__(self, mapping_path: str):
        """
        Initialize unmasker with a mapping file.
        
        Args:
            mapping_path: Path to the JSON mapping file
        """
        with open(mapping_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract mappings (skip metadata)
        self.mappings = {k: v for k, v in data.items() if k != "metadata"}
        self.metadata = data.get("metadata", {})
        
        # Build reverse lookup
        self.reverse_map: Dict[str, str] = {}
        for category, items in self.mappings.items():
            for mask_id, original in items.items():
                self.reverse_map[f"[{mask_id}]"] = original
    
    def unmask_text(self, text: str) -> str:
        """
        Restore original values in masked text.
        
        Args:
            text: Masked text
            
        Returns:
            Text with original values restored
        """
        if not text:
            return text
            
        result = text
        for mask_id, original in self.reverse_map.items():
            result = result.replace(mask_id, original)
        return result
    
    def unmask_csv(self, masked_path: str, output_path: str = None) -> str:
        """
        Restore original values in a masked CSV file.
        
        Args:
            masked_path: Path to masked CSV file
            output_path: Path for restored output (default: input_unmasked.csv)
            
        Returns:
            Path to unmasked file
        """
        masked_path = Path(masked_path)
        if not output_path:
            output_path = masked_path.parent / f"{masked_path.stem}_unmasked{masked_path.suffix}"
        output_path = Path(output_path)
        
        with open(masked_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            rows = [[self.unmask_text(cell) for cell in row] for row in reader]
        
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(rows)
        
        print(f"Unmasked file saved to: {output_path}")
        return str(output_path)


def main():
    parser = argparse.ArgumentParser(
        description="Mask PII in CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Mask a CSV file (creates input_masked.csv and pii_mapping_input.json)
  python pii_masker.py tickets.csv

  # Specify custom output paths
  python pii_masker.py tickets.csv --output clean_tickets.csv --mapping mapping.json

  # Unmask a previously masked file
  python pii_masker.py tickets_masked.csv --unmask --mapping pii_mapping_tickets.json
  
  # Include staff/system emails in masking
  python pii_masker.py tickets.csv --mask-staff-emails
        """
    )
    
    parser.add_argument("input", help="Input CSV file path")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument("--mapping", "-m", help="Mapping JSON file path")
    parser.add_argument("--unmask", "-u", action="store_true", 
                        help="Unmask a previously masked file (requires --mapping)")
    parser.add_argument("--mask-staff-emails", action="store_true",
                        help="Also mask internal/staff email addresses")
    parser.add_argument("--no-mask-names", action="store_true",
                        help="Don't mask names in greetings")
    
    args = parser.parse_args()
    
    if args.unmask:
        if not args.mapping:
            parser.error("--unmask requires --mapping to specify the mapping file")
        unmasker = PIIUnmasker(args.mapping)
        unmasker.unmask_csv(args.input, args.output)
    else:
        masker = PIIMasker(
            mask_staff_emails=args.mask_staff_emails,
            mask_names_in_greetings=not args.no_mask_names
        )
        masker.mask_csv(args.input, args.output, args.mapping)


if __name__ == "__main__":
    main()
