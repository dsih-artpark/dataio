# Terms of Use — DRAFT

> **Draft for internal review. Not approved, not published, not legal advice.**
> Drafted with an AI assistant from how the platform works today (code on branch `ui-redesign`, 3 Nov 2026).
> Items marked **[TO CONFIRM]** need a decision or a fact we could not check from the code.
> The signed-out download pop-up (email + cookie) described here is built (3 Nov 2026).

**Data and AI Commons for One Health**
Run by ARTPARK (AI & Robotics Technology Park), IISc Bengaluru. **[TO CONFIRM: legal entity name and registered address]**

Last updated: **[date of approval]**

---

## 1. About these terms

These terms apply when you use the Data and AI Commons for One Health website, its API, Python SDK, command-line tool, and data assistant (together, "the platform"). By using the platform, or by ticking "I agree" when you download a dataset or create an account, you accept these terms.

If you use the platform on behalf of an organisation, you confirm you are allowed to accept these terms for it.

How we handle personal data is described in our [Privacy Policy](privacy-policy.md).

## 2. What the platform offers

The platform lists datasets about human health, animal health and the environment in India, such as disease records, censuses and weather data, linked by state, district and village.

Each dataset has one of three access levels:

| Access level | What you can do |
| --- | --- |
| **Open download** | See its description, tables, columns and a preview of real rows, and download it. No account needed. |
| **View only** | See its description, tables and columns, and a **synthetic preview** (see section 6). Downloading needs our approval. |
| **Restricted** | See its title, description, coverage and column names only. Access needs our approval. |

You can ask for access to View only and Restricted datasets with the Request access form. We decide each request, and may approve it, refuse it, or approve it with conditions. We usually reply within 3 working days.

## 3. Downloading without an account

To download an Open dataset without an account, we ask for your **email address** and your agreement to these terms and to the dataset's licence. We then save a cookie on your browser so you aren't asked again. Our [Privacy Policy](privacy-policy.md) explains how we use them.

You agree to give a real email address that you control.

## 4. Accounts and API keys

- You can sign in with an email code, Google, GitHub or a passkey. **[TO CONFIRM: which sign-in methods are switched on in production]**
- Keep your account and API keys to yourself. Don't share them, or put them in public code or notebooks. You are responsible for what happens with your keys.
- Tell us at **onehealth@artpark.in** if you think a key has leaked. You can revoke and create keys on your account page.
- Access we grant you is personal to you, or to your organisation if the grant says so. Don't pass data you were given access to on to anyone else, unless its licence or our written approval allows it.

## 5. Dataset licences and credit

- **Each dataset has its own licence**, shown on its page, for example CC BY 4.0. That licence controls how you may use, share and adapt the data. These terms add to it but don't replace it.
- Some datasets show **"Licence: not yet specified"**. **[TO CONFIRM: what applies until a licence is set. Suggested: research and non-commercial use only, with credit, until the licence is published.]**
- **Credit the source** named on the dataset page, and this platform, when you publish or share work that uses the data. Suggested wording: *"Source: [data source], via the Data and AI Commons for One Health (ARTPARK, IISc)."*
- Many datasets come from government departments and other organisations. Their own terms may also apply. Where we know of them, we link to them on the dataset page.
- The platform's **software** is open source under the GNU Affero General Public License v3.0 (AGPL-3.0). That licence covers the code, not the data.

## 6. Synthetic previews

For View only datasets, the preview is **synthetic**. Place names, codes and dates are real, but the numbers are random, and other text is hidden. Synthetic previews show the shape of the data only. **Don't use them for analysis or decisions, and don't present them as real data.**

## 7. The data assistant

- The assistant is an AI tool that helps you find datasets and answer questions about them. **Its answers can be wrong or incomplete.** Check important answers against the dataset itself.
- It is not medical, veterinary, legal or policy advice.
- **Don't type personal or sensitive information** (yours or anyone else's) into the assistant. Messages are processed by a third-party AI provider; see the [Privacy Policy](privacy-policy.md).
- Signed-out visitors can send a limited number of messages per day.

## 8. Acceptable use

You agree **not** to:

1. try to **identify individual people, households or animals' owners** in any dataset, or link datasets in order to do so;
2. get around access levels, rate limits or security, for example by sharing keys, using many accounts, or automating downloads to dodge limits;
3. scrape the website. Use the API instead;
4. upload, send or ask the assistant for anything unlawful, harmful or infringing;
5. use the data in a way that breaks its licence, Indian law, or the law where you are;
6. suggest that ARTPARK, IISc or a data source endorses your work, unless they have agreed in writing;
7. disrupt the platform, or probe it for weaknesses without our written permission. You can report security issues to **onehealth@artpark.in**.

We may limit, suspend or close access, including API keys, if these terms are broken or the platform is being misused.

## 9. Accuracy and no warranty

We collect, standardise and check datasets as described on [How we check quality](/quality). However, data can contain errors, gaps or delays, and sources can change their data.

The platform and all data are provided **"as is"**, without any promise that they are accurate, complete, up to date or fit for a particular purpose. **Don't rely on the platform alone for clinical, public-health emergency or other safety-critical decisions.**

## 10. Liability

As far as the law allows, ARTPARK and IISc are not liable for any loss or damage from using, or not being able to use, the platform or its data, including indirect or consequential loss and lost data or profits. **[TO CONFIRM with legal: liability cap and wording]**

## 11. Changes

We may change these terms. We will post the new version here with a new date. For significant changes, we will also email account holders. If you keep using the platform after a change, you accept the new terms.

We may also change, pause or stop parts of the platform, or remove datasets, for example if a source withdraws them.

## 12. Law and disputes

These terms are governed by the laws of India. Disputes are subject to the courts of Bengaluru, Karnataka. **[TO CONFIRM]**

## 13. Contact

Data and AI Commons for One Health, ARTPARK, IISc Bengaluru
Email: **onehealth@artpark.in**
**[TO CONFIRM: postal address]**
