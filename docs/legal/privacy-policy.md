# Privacy Policy — DRAFT

> **Draft for internal review. Not approved, not published, not legal advice.**
> Drafted with an AI assistant from how the platform works today (code on branch `ui-redesign`, 3 Nov 2026).
> Items marked **[TO CONFIRM]** need a decision or a fact we could not check from the code.
> The signed-out download pop-up works as described here (built 3 Nov 2026): email is **required**, and the IP address
> is **stored with each download** as well as used for download limits.

**Data and AI Commons for One Health**
Run by ARTPARK (AI & Robotics Technology Park), IISc Bengaluru. **[TO CONFIRM: legal entity name and registered address]**

Last updated: **[date of approval]**

---

## 1. Who we are

The Data and AI Commons for One Health ("the platform", "we") is run by ARTPARK, IISc Bengaluru. Under India's Digital Personal Data Protection Act, 2023 ("DPDP Act"), we are the **data fiduciary** for the personal data described here.

This policy is about **personal data of people who use the platform**. It does not cover the datasets in the catalogue. Those are published under their own licences, and we check them so that they don't identify individuals. **[TO CONFIRM: wording on how datasets are de-identified]**

## 2. What we collect and why

| When | What we collect | Why |
| --- | --- | --- |
| **You browse the site** | Nothing that identifies you. Our servers keep standard technical logs (IP address, browser type, page requested, time). | To keep the site running and secure. |
| **You download an Open dataset without an account** | Your **email address**; a **random visitor ID** stored in a cookie; the dataset; the date and time; when you accepted the terms and licence; your **IP address** and browser type. | To understand how many people use each dataset, to contact you about corrections or updates to data you downloaded, to record that you accepted its licence, and to apply download limits and stop misuse. |
| **You request access to a dataset** | Name, email, organisation, organisation type, what you will use the data for, your consent, IP address and browser type. | To decide your request, contact you about it, and keep a record of who was given access and why. |
| **You create an account or sign in** | Email address, display name, and sign-in details: an email code, or your Google or GitHub account email, or passkey data. **[TO CONFIRM: which sign-in methods are on]** | To give you an account and keep it secure. |
| **You use API keys, the SDK or CLI** | The key used, datasets requested, time, IP address and client type. | To give access you are allowed, apply limits, and stop misuse. |
| **You download while signed in** | Your account email, the dataset, the channel (website, SDK, MCP), time, IP address and browser type. | Download statistics and an audit of who accessed which data. |
| **You use the data assistant** | Your messages and the assistant's replies. Signed-in conversations are saved to your account so you can come back to them. Signed-out conversations are not saved by us. Signed-out use is counted per IP address for the daily limit. | To answer your questions and keep the service fair to everyone. |
| **We email you** | Your email address and the message. | Sign-in codes, access-request receipts and decisions, invitations, and notices about these documents. |

We don't sell personal data. We don't use it for advertising. We don't build profiles of you beyond what is described here.

## 3. Cookies and browser storage

| Name | Type | What it does | How long |
| --- | --- | --- | --- |
| `dataio_refresh_token` | Cookie, essential | Keeps you signed in. | 7 days, or until you sign out |
| `oauth_state_*`, `oauth_next_*` | Cookie, essential | Protects Google or GitHub sign-in, and returns you to the right page. | 10 minutes |
| `access_token`, `post_login_redirect` | Browser session storage, essential | Your current sign-in, and the page to return to after signing in. | Until you close the tab |
| `dataio_visitor` | Cookie, only with your agreement | A random ID that remembers you agreed to the terms, so signed-out downloads don't ask again, and lets us count unique downloaders. | About 12 months **[TO CONFIRM]** |

We don't use third-party advertising or tracking cookies. **[TO CONFIRM if analytics are added at launch]**

You can delete cookies in your browser at any time. If you delete the visitor ID, we will ask for your email and agreement again at your next download.

## 4. Who we share it with

We share personal data only with service providers who run parts of the platform for us, and only for that purpose:

| Provider | What for | Where |
| --- | --- | --- |
| **Amazon Web Services (AWS)** | Hosting, database, file storage, and sending email (SES) | Mumbai region, India (ap-south-1). **[TO CONFIRM: SES region in production]** |
| **OpenRouter**, and the AI model provider it routes to (currently Anthropic) | Processing data assistant messages | Outside India, e.g. USA **[TO CONFIRM]** |
| **Google** or **GitHub** | Only if you choose to sign in with them | Their own locations |

When you request access to a dataset, we may share your request with the **organisation that owns that dataset**, if its approval is needed. **[TO CONFIRM]**

We will also share data if the law requires it, or to protect the platform and its users from fraud or misuse.

## 5. How long we keep it

**[TO CONFIRM: all periods below are suggestions]**

| Data | Suggested period |
| --- | --- |
| Download records (signed-out and signed-in) | 3 years, then deleted or made anonymous |
| Access requests | As long as the access lasts, plus 3 years |
| Account data | While your account is open. Deleted within 90 days of you asking us to close it. |
| Saved assistant conversations | Until you delete them or close your account |
| Rate-limit and security logs (IP addresses) | 90 days |

## 6. Your rights

Under the DPDP Act you can:

- **see** the personal data we hold about you, and who we have shared it with;
- **correct or update** it;
- **ask us to delete it**, unless we must keep it by law, or to keep a record of access to restricted data;
- **withdraw your consent**, for example to the visitor cookie or to being contacted about dataset updates. This doesn't affect what we did before you withdrew;
- **nominate** someone to act for you if you die or can't act yourself;
- **complain** to our Grievance Officer, and then to the Data Protection Board of India.

To use these rights, email **onehealth@artpark.in**. We reply within **[30 days — TO CONFIRM]**.

**Grievance Officer:** **[TO CONFIRM: name, role, email]**

## 7. Children

The platform is for adults and organisations. It isn't meant for anyone under 18, and we don't knowingly collect their personal data. If you think a child has given us personal data, contact us and we will delete it.

## 8. Security

We use encrypted connections (HTTPS), short-lived download links, access controls, rate limits and limited staff access. No system is completely secure. If a breach affects your personal data, we will tell you and the Data Protection Board as the DPDP Act requires.

## 9. Changes to this policy

We will post changes here with a new date. For significant changes, we will email account holders and ask again for any consent we need.

## 10. Contact

Data and AI Commons for One Health, ARTPARK, IISc Bengaluru
Email: **onehealth@artpark.in**
**[TO CONFIRM: postal address]**
