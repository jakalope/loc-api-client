# Automatic CAPTCHA Handling - Usage Example

## Fully Automatic Operation

The system now handles CAPTCHA cooling-off periods completely automatically:

```bash
# Start discovery - will run unattended even through CAPTCHA periods
python main.py auto-discover-facets --auto-enqueue --max-items=500

# Example output when CAPTCHA is encountered:
🛑 GLOBAL CAPTCHA DETECTED - Initiating automatic wait-and-resume
   Facet 15 (1901) triggered CAPTCHA protection
   Global cooling-off: 1.0 hours
   Consecutive CAPTCHAs: 1

⏳ Automatically waiting for cooling-off period...
   Discovery will resume at: Sat Jun 28 15:18:26 2025
   Press Ctrl+C if you want to exit early

Cooling-off: 45%|████▌     | 1623/3600s [27:03<33:57, remaining=33.0m]

✅ Cooling-off period completed - resuming discovery!
   Continuing from facet 16 of 50
```

## Key Benefits

- **Zero Monitoring Required**: Start the process and walk away
- **Automatic Resume**: Continues exactly where it left off after cooling-off
- **Progress Preservation**: All discovered items are saved during interruption
- **Visual Progress**: Real-time progress bars show exactly how much time remains
- **Clean Exit**: Ctrl+C at any time to exit gracefully

## For Power Users

Override automatic waiting (risky but available):

```bash
# Skip cooling-off periods (may trigger immediate CAPTCHA again)
python main.py auto-discover-facets --override-captcha
```

## No More Manual Intervention

The old workflow required:
1. Start discovery
2. Wait for CAPTCHA
3. Manually restart after 1 hour
4. Monitor for future CAPTCHAs

The new workflow is:
1. Start discovery
2. Walk away - system handles everything automatically
3. Come back to completed results

This is exactly what was requested: **"I want the app to wait on its own instead of exiting"**