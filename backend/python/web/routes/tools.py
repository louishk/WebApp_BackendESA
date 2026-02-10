"""
Tools routes - utility tools for site management.
"""

from flask import Blueprint, render_template
from flask_login import login_required

from web.auth.decorators import billing_tools_access_required

tools_bp = Blueprint('tools', __name__, url_prefix='/tools')


@tools_bp.route('/billing-date-changer')
@login_required
@billing_tools_access_required
def billing_date_changer():
    """Billing date changer tool page."""
    return render_template('tools/billing_date_changer.html')
