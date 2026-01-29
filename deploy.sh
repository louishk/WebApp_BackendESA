#!/bin/bash
# ============================================
# WebApp Backend Deployment Script
# ============================================
# This script sets up the backend on a fresh server
#
# Usage: sudo ./deploy.sh
#
# Requirements:
# - Ubuntu 22.04 or later
# - Root or sudo access
# - Internet connectivity
# ============================================

set -e

echo "============================================"
echo "WebApp Backend Deployment"
echo "============================================"
echo ""

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "Please run as root or with sudo"
    exit 1
fi

# ─────────────────────────────────────────────────────────────
# 1. Install System Dependencies
# ─────────────────────────────────────────────────────────────
echo "[1/7] Installing system dependencies..."

apt-get update
apt-get install -y \
    apache2 \
    php \
    php-pgsql \
    php-curl \
    php-json \
    php-mbstring \
    php-xml \
    php-zip \
    libapache2-mod-php \
    python3 \
    python3-pip \
    python3-venv \
    postgresql-client \
    curl \
    git

# Enable Apache modules
a2enmod rewrite
a2enmod headers
a2enmod ssl

echo "✓ System dependencies installed"

# ─────────────────────────────────────────────────────────────
# 2. Configure Apache
# ─────────────────────────────────────────────────────────────
echo "[2/7] Configuring Apache..."

# Create Apache virtual host
cat > /etc/apache2/sites-available/webapp-backend.conf << 'EOF'
<VirtualHost *:80>
    ServerName backend.redboxstorage.hk
    DocumentRoot /var/www/html

    <Directory /var/www/html>
        Options -Indexes +FollowSymLinks
        AllowOverride All
        Require all granted
    </Directory>

    # Security headers
    Header always set X-Content-Type-Options "nosniff"
    Header always set X-Frame-Options "SAMEORIGIN"
    Header always set X-XSS-Protection "1; mode=block"

    ErrorLog ${APACHE_LOG_DIR}/webapp-backend-error.log
    CustomLog ${APACHE_LOG_DIR}/webapp-backend-access.log combined
</VirtualHost>
EOF

# Enable site
a2ensite webapp-backend.conf
a2dissite 000-default.conf 2>/dev/null || true

echo "✓ Apache configured"

# ─────────────────────────────────────────────────────────────
# 3. Set Up Application Files
# ─────────────────────────────────────────────────────────────
echo "[3/7] Setting up application files..."

# Create log directory for scheduler
mkdir -p /var/log/pbi-scheduler
chown www-data:www-data /var/log/pbi-scheduler

# Set permissions
chown -R www-data:www-data /var/www/html
find /var/www/html -type d -exec chmod 755 {} \;
find /var/www/html -type f -exec chmod 644 {} \;

# Make shell scripts executable
chmod +x /var/www/html/app/scheduler/python/start_scheduler.sh 2>/dev/null || true
chmod +x /var/www/html/deploy.sh 2>/dev/null || true

echo "✓ Application files configured"

# ─────────────────────────────────────────────────────────────
# 4. Install PHP Dependencies (Composer)
# ─────────────────────────────────────────────────────────────
echo "[4/7] Installing PHP dependencies..."

if [ ! -f /usr/local/bin/composer ]; then
    curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer
fi

cd /var/www/html
sudo -u www-data composer install --no-dev --optimize-autoloader 2>/dev/null || true

echo "✓ PHP dependencies installed"

# ─────────────────────────────────────────────────────────────
# 5. Set Up Python Scheduler
# ─────────────────────────────────────────────────────────────
echo "[5/7] Setting up Python scheduler..."

cd /var/www/html/app/scheduler/python

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install --upgrade pip
pip install -r requirements.txt

deactivate

# Set ownership
chown -R www-data:www-data /var/www/html/app/scheduler/python

echo "✓ Python scheduler configured"

# ─────────────────────────────────────────────────────────────
# 6. Install Systemd Service
# ─────────────────────────────────────────────────────────────
echo "[6/7] Installing systemd service..."

cp /var/www/html/app/scheduler/python/systemd/pbi-scheduler-web.service /etc/systemd/system/

systemctl daemon-reload
systemctl enable pbi-scheduler-web
systemctl start pbi-scheduler-web

echo "✓ Systemd service installed"

# ─────────────────────────────────────────────────────────────
# 7. Restart Services
# ─────────────────────────────────────────────────────────────
echo "[7/7] Restarting services..."

systemctl restart apache2
systemctl restart pbi-scheduler-web

echo "✓ Services restarted"

# ─────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────
echo ""
echo "============================================"
echo "Deployment Complete!"
echo "============================================"
echo ""
echo "Next steps:"
echo "1. Update /var/www/html/.env with your credentials"
echo "2. Run database migration:"
echo "   psql -h esapbi.postgres.database.azure.com -U esa_pbi_admin -d backend -f /var/www/html/sql/setup_postgresql.sql"
echo "3. Optionally migrate data from MySQL:"
echo "   php /var/www/html/sql/migrate_mysql_to_postgresql.php"
echo "4. Test the application at: http://your-server-ip/"
echo ""
echo "Service status:"
systemctl status apache2 --no-pager | head -5
echo ""
systemctl status pbi-scheduler-web --no-pager | head -5
