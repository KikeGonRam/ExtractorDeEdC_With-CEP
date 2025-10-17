Upload SQL helpers for ExtractorDeEdC_With-CEP
===============================================

Scripts to upload `extractor_estados.sql` from this project to your hosting.

Files
- `upload_sql_sftp.ps1` - Uses scp/pscp to upload the SQL file. Prefills host/user from project `.env` if present.
- `upload_sql_ftp.ps1`  - Uses FTP (FtpWebRequest) to upload the SQL file.

Usage (PowerShell)
1. Open PowerShell and cd into the project scripts folder:
   ```powershell
   cd "C:\Users\...\ExtractorDeEdC_With-CEP\scripts"
   ```
2. Run SFTP script (recommended if Hostinger allows SFTP):
   ```powershell
   .\upload_sql_sftp.ps1 -LocalFile "..\extractor_estados.sql"
   ```
   - It will read DB_HOST/DB_USER from `..\.env` as default values.

3. If you only have FTP access, run the FTP script:
   ```powershell
   .\upload_sql_ftp.ps1 -LocalFile "..\extractor_estados.sql"
   ```

After upload
- Login to Hostinger hPanel → phpMyAdmin and import the file from the uploaded location (or import locally from your machine if phpMyAdmin handles it).
- If the file is large and phpMyAdmin fails, use BigDump (bigdump.php) or import via SSH if available.

Security
- Don't store plaintext credentials in scripts. Use prompts or environment variables.
- Delete uploaded SQL from public folders after import.

Need a CLI import command?
- If you tell me you have SSH access to the MySQL server or a shell on the host, I can prepare the exact `mysql` command to run there, using the DB credentials stored in your `.env`.
