from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(to_email, subject, body, from_email, app_password):
    """Sends an email using Gmail SMTP server."""
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "html"))
    
    with smtplib.SMTP("smtp.gmail.com", 587) as server:
        server.starttls()
        server.login(from_email, app_password)
        server.send_message(msg)


def create_fraud_alert_email_body(alert_data):
    """Creates the HTML email body for a fraud card alert."""
    # Color coding based on risk level
    color_map = {
        'HIGH': '#d9534f',    # Red
        'MEDIUM': '#f0ad4e',  # Orange
        'LOW': '#f7ca18'      # Yellow
    }
    risk_color = color_map.get(alert_data.get('risk_level', 'MEDIUM'), '#f0ad4e')
    
    return f"""<html><body style="font-family: Arial, sans-serif;">
            <h2 style="color: {risk_color};">🚨 FRAUD ALERT - {alert_data['risk_level']} RISK</h2>
            <p>Dear {alert_data['customer_name']},</p>
            <p><strong style="color: {risk_color};">We have detected suspicious activity on your card that requires immediate attention.</strong></p>
            
            <div style="background-color: #fff3cd; padding: 15px; border-left: 4px solid {risk_color}; margin: 20px 0;">
            <h3 style="margin-top: 0; color: {risk_color};">Fraud Detection Details</h3>
            <ul style="list-style-type: none; padding-left: 0;">
            <li><strong>Alert ID:</strong> {alert_data['alert_id']}</li>
            <li><strong>Risk Level:</strong> <span style="color: {risk_color}; font-weight: bold;">{alert_data['risk_level']}</span></li>
            <li><strong>Reason:</strong> {alert_data['reason_description']}</li>
            <li><strong>Action Taken:</strong> {alert_data['action']}</li>
            <li><strong>Watchlist Type:</strong> {alert_data['watchlist_type']}</li>
            </ul></div>
            
            <div style="background-color: #f8f9fa; padding: 15px; border-left: 4px solid #6c757d; margin: 20px 0;">
            <h3 style="margin-top: 0;">Transaction Details</h3>
            <ul style="list-style-type: none; padding-left: 0;">
            <li><strong>Transaction ID:</strong> {alert_data['transaction_id']}</li>
            <li><strong>Card Number:</strong> **** **** **** {alert_data['card_last_4']}</li>
            <li><strong>Amount:</strong> {alert_data['currency']} {alert_data['amount']:,.2f}</li>
            <li><strong>Merchant:</strong> {alert_data['merchant_name']} ({alert_data['merchant_category']})</li>
            <li><strong>Location:</strong> {alert_data['transaction_city']}, {alert_data['transaction_country']}</li>
            <li><strong>Date/Time:</strong> {alert_data['transaction_timestamp']}</li>
            <li><strong>Payment Channel:</strong> {alert_data['payment_channel']}</li>
            <li><strong>Status:</strong> {alert_data['transaction_status']}</li>
            </ul></div>
            
            <div style="background-color: #f2dede; padding: 15px; border-left: 4px solid #d9534f; margin: 20px 0;">
            <h3 style="margin-top: 0; color: #d9534f;">⚠️ IMMEDIATE ACTION REQUIRED</h3>
            <p><strong>If you did NOT authorize this transaction:</strong></p>
            <ul>
            <li>Call our 24/7 Fraud Hotline immediately: <strong style="font-size: 16px;">1-800-FRAUD-911</strong></li>
            <li>Do NOT use your card until you speak with our fraud team</li>
            <li>Review your recent transactions in the mobile app</li>
            </ul>
            <p><strong>If you authorized this transaction:</strong></p>
            <ul>
            <li>Reply to this email or call customer service to confirm</li>
            <li>We may need to verify your identity for security purposes</li>
            </ul>
            </div>
            
            <div style="background-color: #f8f9fa; padding: 15px; margin: 20px 0;">
            <h4 style="margin-top: 0;">Additional Information</h4>
            <ul style="list-style-type: none; padding-left: 0; font-size: 12px;">
            <li><strong>Watchlist ID:</strong> {alert_data['watchlist_id']}</li>
            <li><strong>Reported By:</strong> {alert_data['reported_by']}</li>
            <li><strong>Reported Source:</strong> {alert_data['reported_source']}</li>
            <li><strong>Watchlist Effective From:</strong> {alert_data['watchlist_effective_from']}</li>
            </ul></div>
            
            <p>For your protection, we are monitoring your account continuously.</p>
            <hr style="margin: 20px 0; border: none; border-top: 1px solid #ddd;">
            <p style="font-size: 12px; color: #6c757d;">
            This is an automated fraud alert from FinGuard Security Monitoring System.<br>
            Do not reply to this email. Contact our fraud hotline for assistance.
            </p>
            </body></html>"""


# Get configuration outside the foreach_batch_sink for serialization
email_from = "youremail@gmail.com"
try:
    # Taking the secrets and bootstrapservers from the secret scope
    app_password = dbutils().secrets().get("finguard-scope", "gmail_api_key")
except Exception as e:
    print(f"❌ Failed to retrieve Gmail API key from secrets: {e}")
    app_password = None

# For each Micro batch it will take each data and calls the send email function to send the emails
@dp.foreach_batch_sink(name="fraud_email_notifier_sink")
def send_fraud_alert_emails(df, batch_id):
    """ForEachBatch sink that sends email alerts for fraud card alerts."""
    
    if app_password is None:
        print(f"❌ Batch {batch_id}: Gmail API key not available, skipping email notifications")
        return
    
    rows = df.collect()
    print(f"🚨 Batch {batch_id}: Processing {len(rows)} fraud alert(s)...")
    
    success_count = 0
    failure_count = 0
    
    for row in rows:
        try:
            # Extract last 4 digits of card number
            card_last_4 = row.card_number[-4:] if row.card_number else "XXXX"
            
            alert_data = {
                'alert_id': row.alert_id,
                'customer_name': row.customer_name,
                'risk_level': row.risk_level,
                'reason_description': row.reason_description,
                'action': row.action,
                'watchlist_type': row.watchlist_type,
                'transaction_id': row.transaction_id,
                'card_last_4': card_last_4,
                'amount': float(row.amount),
                'currency': row.currency,
                'merchant_name': row.merchant_name,
                'merchant_category': row.merchant_category,
                'transaction_city': row.transaction_city,
                'transaction_country': row.transaction_country,
                'transaction_timestamp': str(row.transaction_timestamp),
                'payment_channel': row.payment_channel,
                'transaction_status': row.transaction_status,
                'watchlist_id': row.watchlist_id,
                'reported_by': row.reported_by,
                'reported_source': row.reported_source,
                'watchlist_effective_from': str(row.watchlist_effective_from)
            }
            
            subject = f"🚨 FRAUD ALERT - {alert_data['risk_level']} Risk - {alert_data['alert_id']}"
            body = create_fraud_alert_email_body(alert_data)
            
            send_email(row.customer_email_id, subject, body, email_from, app_password)
            
            success_count += 1
            print(f"  ✅ Fraud alert email sent to {row.customer_email_id} for transaction {row.transaction_id}")
            
        except Exception as e:
            failure_count += 1
            print(f"  ❌ Error processing fraud alert {row.alert_id}: {e}")
    
    print(f"📊 Batch {batch_id} complete: {success_count} succeeded, {failure_count} failed")

# Reads the streaming data from the gold layer fraud alert table and sends the data in Micro batches
@dp.append_flow(target="fraud_email_notifier_sink")
def fraud_alert_stream():
    """Streaming flow that reads fraud card alerts."""
    return spark.readStream.table("finguard.gold.fraud_card_alert")