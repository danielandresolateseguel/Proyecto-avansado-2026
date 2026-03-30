import os
from flask import Flask
from dotenv import load_dotenv
from . import database
from werkzeug.exceptions import HTTPException

load_dotenv()

def create_app(test_config=None):
    # Create and configure the app
    # static_folder='../' means serve files from project root
    app = Flask(__name__, instance_relative_config=True, static_folder='../', static_url_path='/static')
    
    # Configure JSON provider to handle datetime as ISO8601
    import json
    from datetime import datetime
    
    class CustomJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return super().default(obj)

    try:
        from flask.json.provider import DefaultJSONProvider
        class CustomJSONProvider(DefaultJSONProvider):
            def default(self, obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return super().default(obj)
        app.json = CustomJSONProvider(app)
    except ImportError:
        app.json_encoder = CustomJSONEncoder

    # Default config
    app.config.from_mapping(
        SECRET_KEY=os.getenv('SECRET_KEY', 'dev'),
        DATABASE=os.getenv('DATABASE_PATH', os.path.join(app.root_path, '..', 'orders.db')),
        CONFIG_DIR=os.path.join(app.root_path, '..', 'config'),
        JSON_AS_ASCII=True,  # Force ASCII JSON to avoid encoding issues with emojis
        SEND_FILE_MAX_AGE_DEFAULT=0,
        # Security: Session Cookie Configuration
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE='Lax',
    )
    
    # Enable Secure Cookie if in production (requires HTTPS)
    if os.environ.get('FLASK_ENV') == 'production':
        app.config['SESSION_COOKIE_SECURE'] = True

    if test_config is None:
        # Load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # Load the test config if passed in
        app.config.from_mapping(test_config)

    # Ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    # Initialize DB
    database.init_app(app)
    
    # Initialize DB tables and seed data
    with app.app_context():
        database.init_db()
        database.seed_products_from_config(app.config['CONFIG_DIR'])
        database.backfill_product_details_from_config(app.config['CONFIG_DIR'])
        database.backfill_product_variants_from_config(app.config['CONFIG_DIR'])
        database.backfill_product_images_from_config(app.config['CONFIG_DIR'])
        database.seed_admin_users_from_env(app.config['CONFIG_DIR'])

    # Start background tasks
    from .tasks import start_background_tasks
    start_background_tasks(app)

    # Register Blueprints
    from .blueprints import auth, orders, cash, products, carousel, public, archive, tenants, system
    
    app.register_blueprint(auth.bp)
    app.register_blueprint(orders.bp)
    app.register_blueprint(cash.bp)
    app.register_blueprint(products.bp)
    app.register_blueprint(carousel.bp)
    app.register_blueprint(archive.bp)
    app.register_blueprint(tenants.bp)
    app.register_blueprint(system.bp)

    # Register public last to avoid catching API routes
    app.register_blueprint(public.bp)

    @app.route('/')
    def root():
        return {'status': 'ok', 'message': 'Root application running', 'version': '1.0.14'}

    @app.errorhandler(500)
    def internal_error(error):
        return {'error': 'Internal Server Error', 'message': str(error)}, 500

    @app.errorhandler(Exception)
    def handle_exception(e):
        # pass through HTTP errors
        if isinstance(e, HTTPException):
            return e
        # now you're handling non-HTTP exceptions only
        return {'error': 'Unexpected Error', 'message': str(e)}, 500

    return app
