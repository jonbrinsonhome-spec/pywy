import requests
import json
import time
import random
import os
import logging
import urllib3
from datetime import datetime, timedelta
import re
import mimetypes
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from gradio_client import Client, handle_file
from huggingface_hub import login
import colorlog
from colorlog import ColoredFormatter
import threading
import hashlib
import queue
import warnings
# Remove fcntl import and use cross-platform locking
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import traceback
import sys
warnings.filterwarnings('ignore', category=urllib3.exceptions.InsecureRequestWarning)

# ========== HELPER FUNCTIONS ==========

def should_use_legacy_system(account_username: str) -> str:
    """Check which messaging system(s) an account should use
    Returns: 'legacy', 'standard', or 'both'
    """
    # First check if account is in accounts3.txt (BOTH systems)
    if os.path.exists('accounts3.txt'):
        try:
            with open('accounts3.txt', 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and ':' in line:
                        username, _ = line.split(':', 1)
                        if username.strip() == account_username:
                            logger.debug(f"✅ Account {account_username} uses BOTH systems (found in accounts3.txt)")
                            return 'both'
        except Exception as e:
            logger.warning(f"⚠️ Error checking accounts3.txt: {e}")
    
    # Check if account is in accounts2.txt (legacy system)
    if os.path.exists('accounts2.txt'):
        try:
            with open('accounts2.txt', 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and ':' in line:
                        username, _ = line.split(':', 1)
                        if username.strip() == account_username:
                            logger.debug(f"✅ Account {account_username} uses legacy system (found in accounts2.txt)")
                            return 'legacy'
        except Exception as e:
            logger.warning(f"⚠️ Error checking accounts2.txt: {e}")
    
    # Default to standard system (accounts.txt)
    logger.debug(f"📤 Account {account_username} uses standard system")
    return 'standard'
    
def debug_trace(message: str):
    """Debug tracing function to track the flow of use_legacy_system flag"""
    logger.debug(f"🔍 TRACE: {message}")

def setup_colored_logging():
    logger_name = "RedditMasterBot"
    existing = logging.getLogger(logger_name)
    if existing.handlers:
        return existing
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    
    import sys
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    
    formatter = ColoredFormatter(
        "%(log_color)s[%(asctime)s] %(levelname)-7s %(message)s",
        datefmt="%m/%d/%y %H:%M:%S",
        reset=True,
        log_colors={'DEBUG':'cyan','INFO':'green','WARNING':'yellow','ERROR':'red','CRITICAL':'red,bg_white'},
        style='%'
    )
    stream_handler = colorlog.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    file_handler = logging.FileHandler("reddit_master_bot.log", encoding='utf-8')
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    return logger

logger = setup_colored_logging()


# ========== DATABASE MANAGER ==========
import psycopg2
import psycopg2.extras
from contextlib import contextmanager
import redis

class DatabaseManager:
    """Manages PostgreSQL database connections"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._initialized = True
            self.connection_params = {
                'host': 'localhost',
                'database': 'reddit_bot_db',
                'user': 'reddit_bot_user',
                'password': 'BotUserPass123!',
                'port': 5432
            }
            logger.info("📊 Database Manager initialized")
    
    @contextmanager
    def get_connection(self):
        """Get a database connection with automatic cleanup"""
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            yield conn
        except Exception as e:
            logger.error(f"❌ Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self):
        """Get a database cursor with automatic cleanup"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            try:
                yield cursor
                conn.commit()
            except Exception as e:
                conn.rollback()
                logger.error(f"❌ Database cursor error: {e}")
                raise
            finally:
                cursor.close()

class RedisManager:
    """Manages Redis connections for distributed coordination"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RedisManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._initialized = True
            try:
                self.redis_client = redis.Redis(
                    host='localhost',
                    port=6379,
                    decode_responses=True
                )
                # Test connection - if it fails due to auth, try without password
                self.redis_client.ping()
                logger.info("🔴 Redis Manager initialized (without password)")
            except redis.exceptions.AuthenticationError:
                # If authentication error, try without password
                self.redis_client = redis.Redis(
                    host='localhost',
                    port=6379,
                    decode_responses=True
                )
                logger.info("🔴 Redis Manager initialized (no authentication)")
            except Exception as e:
                logger.error(f"❌ Redis connection error: {e}")
                # Create a dummy client that won't fail
                self.redis_client = None
    
    def get_client(self):
        """Get Redis client"""
        return self.redis_client

# Initialize managers
db_manager = DatabaseManager()
redis_manager = RedisManager()

# ========== GLOBAL PROCESSED USERS MANAGER (DATABASE VERSION) ==========
class GlobalProcessedUsersManager:
    """Manages globally processed users in database to prevent duplicate messaging"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GlobalProcessedUsersManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        with self._lock:
            if not hasattr(self, '_initialized') or not self._initialized:
                self._initialized = True
                # Access redis_client directly instead of using get_client()
                self.redis_client = redis_manager.redis_client
                logger.info("🌍 Global Processed Users Manager initialized (Database Version)")
    
    def reserve_user_for_processing(self, user_identifier: str, account_username: str) -> bool:
        """
        Reserve a user for processing by a specific account using atomic database operations.
        Returns True if user can be processed, False if already being processed.
        """
        lock_key = f"user_lock:{user_identifier}"
        
        # Try to acquire Redis lock (expires in 300 seconds)
        acquired = self.redis_client.set(lock_key, account_username, nx=True, ex=300)
        
        if not acquired:
            # Check if WE already have the lock
            current_holder = self.redis_client.get(lock_key)
            if current_holder == account_username:
                # We already have the lock from a previous attempt
                logger.debug(f"🔒 [{account_username}] Already have lock for {user_identifier}")
                return True
            logger.debug(f"🔒 [{account_username}] User {user_identifier} already locked by another instance")
            return False
        
        # CRITICAL FIX: Add immediate check after getting Redis lock
        # This prevents race condition between Redis and database
        try:
            # First, check if ANY other account has already processed this user
            with db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    # Try to lock the row for this user in the database
                    cursor.execute("""
                        SELECT account_username FROM global_processed_users 
                        WHERE user_identifier = %s
                        FOR UPDATE NOWAIT
                    """, (user_identifier,))
                    
                    existing_record = cursor.fetchone()
                    
                    if existing_record:
                        # User already processed by SOME account
                        other_account = existing_record[0]
                        conn.rollback()
                        self.redis_client.delete(lock_key)  # Release Redis lock
                        
                        if other_account == account_username:
                            logger.debug(f"📭 [{account_username}] User {user_identifier} already processed by us")
                        else:
                            logger.debug(f"📭 [{account_username}] User {user_identifier} already processed by {other_account}")
                        return False
                    
                    # Check if WE have processed this user
                    cursor.execute("""
                        SELECT 1 FROM global_processed_users 
                        WHERE user_identifier = %s AND account_username = %s
                    """, (user_identifier, account_username))
                    
                    if cursor.fetchone():
                        # User already processed by this account
                        conn.rollback()
                        self.redis_client.delete(lock_key)  # Release Redis lock
                        logger.debug(f"📭 [{account_username}] User {user_identifier} already processed by this account")
                        return False
                    
                    # Insert the reservation atomically
                    current_time = datetime.now()
                    cursor.execute("""
                        INSERT INTO global_processed_users 
                        (user_identifier, account_username, processed_at, status)
                        VALUES (%s, %s, %s, 'processing')
                    """, (user_identifier, account_username, current_time))
                    
                    conn.commit()
                    logger.info(f"🔒 [{account_username}] Reserved user {user_identifier} for processing")
                    return True
                    
        except psycopg2.errors.LockNotAvailable:
            # Couldn't get database lock, another instance is processing
            conn.rollback() if 'conn' in locals() and conn else None
            self.redis_client.delete(lock_key)
            logger.debug(f"🔒 [{account_username}] User {user_identifier} locked by another instance (database)")
            return False
        except Exception as e:
            # Rollback on any error
            conn.rollback() if 'conn' in locals() and conn else None
            self.redis_client.delete(lock_key)
            logger.error(f"❌ [{account_username}] Error reserving user {user_identifier}: {e}")
            return False
    
    def release_user_reservation(self, user_identifier: str, account_username: str, success: bool):
        """
        Release a user reservation and mark as processed if successful.
        """
        lock_key = f"user_lock:{user_identifier}"
        
        try:
            with db_manager.get_cursor() as cursor:
                if success:
                    # Update status to 'processed' if successful
                    cursor.execute("""
                        UPDATE global_processed_users 
                        SET status = 'processed', processed_at = NOW()
                        WHERE user_identifier = %s AND account_username = %s
                    """, (user_identifier, account_username))
                    
                    logger.info(f"✅ [{account_username}] Marked user {user_identifier} as processed globally")
                else:
                    # Remove the reservation if not processed
                    cursor.execute("""
                        DELETE FROM global_processed_users 
                        WHERE user_identifier = %s AND account_username = %s AND status = 'processing'
                    """, (user_identifier, account_username))
                    
                    logger.info(f"🔓 [{account_username}] Released reservation for {user_identifier} (not processed)")
            
            # Release Redis lock
            self.redis_client.delete(lock_key)
                
        except Exception as e:
            logger.error(f"❌ [{account_username}] Error releasing reservation for {user_identifier}: {e}")
            # Still try to release Redis lock
            try:
                self.redis_client.delete(lock_key)
            except:
                pass
    
    def can_process_user(self, user_identifier: str, account_username: str) -> bool:
        """
        Check if a user can be processed (not processed globally AND not in progress).
        """
        lock_key = f"user_lock:{user_identifier}"
        
        # Check Redis lock first (fast)
        if self.redis_client.exists(lock_key):
            logger.debug(f"🔒 [{account_username}] User {user_identifier} is currently being processed")
            return False
        
        # Check database
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM global_processed_users 
                    WHERE user_identifier = %s AND account_username = %s
                    LIMIT 1
                """, (user_identifier, account_username))
                
                if cursor.fetchone():
                    logger.debug(f"📭 [{account_username}] User {user_identifier} already processed")
                    return False
                
                return True
                
        except Exception as e:
            logger.error(f"❌ [{account_username}] Error checking user {user_identifier}: {e}")
            return False
    
    def cleanup_old_users(self, max_age_days: int = 90):
        """Clean up old processed users from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    DELETE FROM global_processed_users 
                    WHERE account_username = %s 
                    AND processed_at < NOW() - INTERVAL '%s days'
                """, (self.account_username, max_age_days))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"🧹 [{self.account_username}] Cleaned up {deleted_count} old processed users from database")
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error cleaning up old users: {e}")

global_processed_manager = GlobalProcessedUsersManager()

# ========== DATABASE REFRESH COORDINATOR ==========
class RefreshCoordinator:
    """Coordinates refresh operations across multiple instances using database"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(RefreshCoordinator, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if not hasattr(self, '_initialized') or not self._initialized:
            self._initialized = True
            # Access redis_client directly instead of using get_client()
            self.redis_client = redis_manager.redis_client
            self.min_refresh_interval = 60
            logger.info("🔄 Refresh Coordinator initialized (Database Version)")
    
    def request_refresh(self, account_username: str) -> bool:
        """
        Request permission to perform a refresh using database.
        Returns True if refresh can proceed, False if should wait.
        """
        refresh_lock_key = f"refresh_lock:{account_username}"
        
        # Try to acquire Redis lock for this account
        acquired = self.redis_client.set(refresh_lock_key, "1", nx=True, ex=600)  # 10 minute lock
        
        if not acquired:
            logger.debug(f"⏳ [{account_username}] Refresh already in progress by another instance")
            return False
        
        try:
            # Check last refresh time in database
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT requested_at FROM refresh_coordination 
                    WHERE account_username = %s AND status = 'completed'
                    ORDER BY requested_at DESC LIMIT 1
                """, (account_username,))
                
                result = cursor.fetchone()
                
                if result:
                    last_refresh = result['requested_at']
                    # Check if it's already a timestamp
                    if isinstance(last_refresh, datetime):
                        last_refresh_seconds = last_refresh.timestamp()
                    else:
                        # If stored as milliseconds, convert to seconds
                        if last_refresh > 1000000000000:
                            last_refresh_seconds = last_refresh / 1000
                        else:
                            last_refresh_seconds = last_refresh
                    
                    time_since_last = (datetime.now().timestamp() - last_refresh_seconds)
                    
                    if time_since_last < self.min_refresh_interval:
                        wait_time = self.min_refresh_interval - time_since_last
                        logger.info(f"⏳ [{account_username}] Waiting {wait_time:.1f}s before refresh (rate limit)")
                        # Release lock since we're waiting
                        self.redis_client.delete(refresh_lock_key)
                        return False
                
                # Record refresh request - use datetime for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO refresh_coordination (account_username, status, requested_at)
                    VALUES (%s, 'in_progress', %s)
                """, (account_username, current_time))
                
                logger.info(f"✅ [{account_username}] Refresh request approved")
                return True
                
        except Exception as e:
            # Release lock on error
            self.redis_client.delete(refresh_lock_key)
            logger.error(f"❌ [{account_username}] Error requesting refresh: {e}")
            return False
    
    def release_refresh_lock(self, account_username: str):
        """Release the refresh lock and mark as completed"""
        refresh_lock_key = f"refresh_lock:{account_username}"
        
        try:
            # Update database
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    UPDATE refresh_coordination 
                    SET status = 'completed', completed_at = NOW()
                    WHERE account_username = %s AND status = 'in_progress'
                    ORDER BY requested_at DESC LIMIT 1
                """, (account_username,))
            
            # Release Redis lock
            self.redis_client.delete(refresh_lock_key)
            logger.debug(f"🔓 [{account_username}] Released refresh lock")
            
        except Exception as e:
            logger.error(f"❌ [{account_username}] Error releasing refresh lock: {e}")

refresh_coordinator = RefreshCoordinator()

            
            
# ========== SUBREDDIT POST SCRAPER (METHOD 3) ==========
class SubredditPostScraper:
    """Scrapes posters from specific NSFW subreddits (Method 3)"""
    
    def __init__(self, account_username: str, session: requests.Session):
        self.account_username = account_username
        self.session = session
        
        # Target subreddits for scraping posters
        self.subreddits = [
            'AskDickPic',
            'AgeGapPersonals',
            'FastSexting',
            'JerkOffChat',
            'dirtyr4r',
            'DirtyChatPals',
            'dirtypenpals'
        ]
        
        # Store scraped posters
        self.scraped_posters_file = Path(f"scraped_posters_{account_username}.json")
        self.scraped_posters = self._load_scraped_posters()
        
        logger.info(f"📝 [{self.account_username}] Subreddit poster scraper initialized for {len(self.subreddits)} subreddits")
    
    def _load_scraped_posters(self) -> set:
        """Load previously scraped posters from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT poster_id FROM scraped_posters 
                    WHERE account_username = %s
                """, (self.account_username,))
                
                results = cursor.fetchall()
                posters = {row['poster_id'] for row in results}
                logger.info(f"📁 [{self.account_username}] Loaded {len(posters)} previously scraped posters from database")
                return posters
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error loading scraped posters: {e}")
            return set()
    
    def is_poster_scraped(self, poster_id: str) -> bool:
        """Check if a poster has already been scraped in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM scraped_posters 
                    WHERE poster_id = %s AND account_username = %s
                    LIMIT 1
                """, (poster_id, self.account_username))
                
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking scraped poster: {e}")
            return False
    
    def mark_poster_scraped(self, username: str, post_id: str, subreddit: str):
        """Mark a poster as scraped in database"""
        poster_id = f"{username}_{subreddit}_{post_id}"
        
        try:
            with db_manager.get_cursor() as cursor:
                # Use datetime.now() for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO scraped_posters 
                    (poster_id, account_username, username, post_id, subreddit, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (poster_id, account_username) DO NOTHING
                """, (poster_id, self.account_username, username, post_id, subreddit, current_time))
                
            logger.debug(f"📝 [{self.account_username}] Marked poster {username} from r/{subreddit} as scraped in database")
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking poster scraped: {e}")
    
    def scrape_posters_from_subreddits(self, max_post_age_minutes: int = 10, max_posts_per_sub: int = 20) -> list:
        """
        Scrapes posters from target subreddits
        Returns posters from the last 10 minutes
        Returns the FIRST available poster we can actually message
        """
        current_time = time.time()
        
        logger.info(f"🔍 [{self.account_username}] Scraping posters from {len(self.subreddits)} subreddits")
        
        # Shuffle subreddits to distribute load
        subreddits_to_scrape = self.subreddits.copy()
        random.shuffle(subreddits_to_scrape)
        
        for subreddit_index, subreddit_name in enumerate(subreddits_to_scrape):
            try:
                logger.info(f"🌐 [{self.account_username}] Checking r/{subreddit_name} for posters...")
                
                url = f"https://www.reddit.com/r/{subreddit_name}/new.json"
                params = {
                    'limit': max_posts_per_sub,
                    'raw_json': 1
                }
                
                # Add delay to avoid rate limiting
                time.sleep(1.5)
                
                response = self.session.get(url, params=params, timeout=20, verify=False)
                
                if response.status_code != 200:
                    if response.status_code == 429:
                        logger.warning(f"⏳ [{self.account_username}] Rate limited on r/{subreddit_name}, waiting 30 seconds")
                        time.sleep(30)
                        continue
                    logger.error(f"❌ [{self.account_username}] Failed to scrape r/{subreddit_name}: {response.status_code}")
                    continue
                
                data = response.json()
                posts = data['data']['children']
                
                if not posts:
                    logger.debug(f"📭 [{self.account_username}] No posts found in r/{subreddit_name}")
                    continue
                
                logger.debug(f"📊 [{self.account_username}] Found {len(posts)} posts in r/{subreddit_name}")
                
                # Track statistics
                posts_checked = 0
                posters_skipped = {
                    'too_old': 0,
                    'already_scraped': 0,
                    'bot_mod': 0,
                    'not_eligible': 0,
                    'already_processed': 0
                }
                
                for post_index, post in enumerate(posts):
                    posts_checked += 1
                    post_data = post['data']
                    
                    # Extract post information
                    post_id = post_data['id']
                    post_title = post_data['title'][:150]
                    post_author = post_data['author']
                    post_created = post_data.get('created_utc', 0)
                    post_selftext = post_data.get('selftext', '')
                    post_score = post_data.get('score', 0)
                    post_permalink = post_data.get('permalink', '')
                    
                    # Skip old posts (older than 2 hours)
                    post_age_hours = (current_time - post_created) / 3600
                    if post_age_hours > 2:
                        posters_skipped['too_old'] += 1
                        logger.debug(f"⏰ [{self.account_username}] Skipping old post in r/{subreddit_name} ({post_age_hours:.1f}h)")
                        continue
                    
                    # Skip AutoModerator, deleted, or removed posts
                    if post_author in ['AutoModerator', '[deleted]', '[removed]']:
                        posters_skipped['bot_mod'] += 1
                        continue
                    
                    # Skip posts that are too short or likely spam
                    if len(post_selftext.strip()) < 10 and len(post_title.strip()) < 10:
                        posters_skipped['not_eligible'] += 1
                        continue
                    
                    # Apply subreddit-specific filtering
                    if not self._is_eligible_poster(post_author, post_title, post_selftext, subreddit_name):
                        posters_skipped['not_eligible'] += 1
                        logger.debug(f"🚫 [{self.account_username}] Poster {post_author} not eligible for r/{subreddit_name}")
                        continue
                    
                    # Check if poster already processed
                    poster_id = f"{post_author}_{subreddit_name}_{post_id}"
                    if self.is_poster_scraped(poster_id):
                        posters_skipped['already_scraped'] += 1
                        logger.debug(f"📭 [{self.account_username}] Already scraped poster: {post_author}")
                        continue
                    
                    # Get user's recent posts for context
                    recent_posts = self._get_user_recent_posts(post_author, limit=2)
                    
                    # CRITICAL FIX: Check if user is available BEFORE marking as eligible
                    # Check if poster already processed globally or by another account
                    if not global_processed_manager.can_process_user(post_author, self.account_username):
                        posters_skipped['already_processed'] += 1
                        logger.debug(f"⏭️  [{self.account_username}] Skipping {post_author} - already processed globally or in progress")
                        continue
                    
                    # Try to reserve this user for processing
                    if not global_processed_manager.reserve_user_for_processing(post_author, self.account_username):
                        posters_skipped['already_processed'] += 1
                        logger.debug(f"⏭️  [{self.account_username}] User {post_author} was taken by another account just now")
                        continue
                    
                    # NOW we have a truly eligible poster - we can message them!
                    poster_info = {
                        'poster_username': post_author,
                        'post_id': post_id,
                        'subreddit': subreddit_name,
                        'post_title': post_title,
                        'post_content': post_selftext[:500],  # Limit content length
                        'post_age_hours': post_age_hours,
                        'post_created_utc': post_created,
                        'post_score': post_score,
                        'post_permalink': post_permalink,
                        'recent_posts': recent_posts,
                        'scraper_id': f"{post_author}_{subreddit_name}_{post_id}",
                        'scrape_method': 'subreddit_poster'  # Mark as method 3
                    }
                    
                    # Mark as scraped
                    self.mark_poster_scraped(post_author, post_id, subreddit_name)
                    
                    logger.info(f"✅ [{self.account_username}] Found AVAILABLE poster: {post_author} from r/{subreddit_name}")
                    logger.info(f"📊 [{self.account_username}] Stopping search after checking {subreddit_index + 1} subreddits")
                    
                    # Return just this one poster - we found what we need!
                    return [poster_info]
                
                logger.info(f"📭 [{self.account_username}] No available posters found in r/{subreddit_name}")
                
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Error scraping r/{subreddit_name}: {e}")
                continue
        
        logger.info(f"📭 [{self.account_username}] No available posters found in any subreddit")
        return []
    
    def _is_eligible_poster(self, username: str, title: str, content: str, subreddit: str) -> bool:
        """Check if a poster is eligible based on subreddit-specific rules"""
        combined_text = f"{title.lower()} {content.lower()}"
        
        # Common exclusion patterns
        exclusion_patterns = [
            'f4m', 'f4f', 'f4a', 'f4r',  # Female looking for...
            'female4', 'girl4', 'woman4',
            '[f4', '(f4', '{f4',
            'looking for m', 'looking for male', 'looking for man',
            'seeking m', 'seeking male', 'seeking man',
            'want m', 'want male', 'want man'
        ]
        
        for pattern in exclusion_patterns:
            if pattern in combined_text:
                return False
        
        # Subreddit-specific eligibility checks
        if subreddit == 'AskDickPic':
            # Look for requests for dick ratings/opinions
            dick_patterns = ['rate my', 'opinion on', 'thoughts on', 'how is my', 'what do you think']
            return any(pattern in combined_text for pattern in dick_patterns)
        
        elif subreddit == 'AgeGapPersonals':
            # Look for age gap seeking posts
            age_patterns = ['older', 'younger', 'age gap', 'mature', 'daddy', 'mommy']
            return any(pattern in combined_text for pattern in age_patterns)
        
        elif subreddit == 'FastSexting':
            # Look for M4F, M4A patterns
            m4_patterns = ['m4f', 'm4a', 'm4r', 'male4', 'man4', 'guy4']
            return any(pattern in combined_text for pattern in m4_patterns)
        
        else:  # JerkOffChat, dirtyr4r, DirtyChatPals, dirtypenpals
            # Look for chat/connection seeking posts
            chat_patterns = ['chat', 'talk', 'connect', 'message', 'conversation']
            return any(pattern in combined_text for pattern in chat_patterns)
    
    def _get_user_recent_posts(self, username: str, limit: int = 2) -> list:
        """Get user's recent posts (for context in message generation)"""
        try:
            url = f"https://www.reddit.com/user/{username}/submitted.json"
            params = {
                'limit': limit,
                'raw_json': 1
            }
            
            # Add delay to avoid rate limiting
            time.sleep(1.5)
            
            response = self.session.get(url, params=params, timeout=15, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                posts_data = data['data']['children']
                
                recent_posts = []
                for post in posts_data:
                    if post['kind'] == 't3':
                        post_info = post['data']
                        recent_posts.append({
                            'title': post_info.get('title', ''),
                            'subreddit': post_info.get('subreddit', 'Unknown'),
                            'content': post_info.get('selftext', '')[:200],
                            'created_utc': post_info.get('created_utc', 0),
                            'score': post_info.get('score', 0)
                        })
                
                return recent_posts
            else:
                if response.status_code == 429:
                    logger.debug(f"⏳ [{self.account_username}] Rate limited getting user posts for {username}")
                    time.sleep(30)
                logger.debug(f"📭 [{self.account_username}] No recent posts found for user {username}")
                return []
                
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error getting recent posts for {username}: {e}")
            return []
    
    def cleanup_old_scraped_posters(self, max_age_days: int = 7):
        """Clean up old scraped posters"""
        try:
            # Simple cleanup - just keep last 1000 entries to prevent file bloat
            if len(self.scraped_posters) > 1000:
                posters_list = list(self.scraped_posters)
                self.scraped_posters = set(posters_list[-1000:])
                self._save_scraped_posters()
                logger.info(f"🧹 [{self.account_username}] Cleaned up scraped posters, kept {len(self.scraped_posters)} most recent")
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error cleaning up scraped posters: {e}")





# ========== COMMUNITY FEED SCRAPER 3 (HOT FEED METHOD 4) ==========
class CommunityFeedScraperHot1:
    """Scrapes active commenters from the first community feed (all_nsfw) HOT feed"""
    
    def __init__(self, account_username: str, session: requests.Session):
        self.account_username = account_username
        self.session = session
        self.feed_url = "https://www.reddit.com/user/rileyxwood/m/all_nsfw/hot/.json"
        
        # Store scraped commenters to avoid duplicates
        self.scraped_commenters_file = Path(f"scraped_commenters_hot1_{account_username}.json")
        self.scraped_commenters = self._load_scraped_commenters()
        
        logger.info(f"🔥 [{self.account_username}] First community HOT feed scraper initialized (5-hour search window)")
    
    def _load_scraped_commenters(self) -> set:
        """Load previously scraped commenters from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT commenter_id FROM scraped_commenters 
                    WHERE account_username = %s
                """, (self.account_username,))
                
                results = cursor.fetchall()
                commenters = {row['commenter_id'] for row in results}
                logger.info(f"📁 [{self.account_username}] Loaded {len(commenters)} previously scraped HOT1 commenters from database")
                return commenters
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error loading scraped HOT1 commenters: {e}")
            return set()
    
    def is_commenter_scraped(self, commenter_id: str) -> bool:
        """Check if a commenter has already been scraped in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM scraped_commenters 
                    WHERE commenter_id = %s AND account_username = %s
                    LIMIT 1
                """, (commenter_id, self.account_username))
                
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking scraped HOT1 commenter: {e}")
            return False
    
    def mark_commenter_scraped(self, username: str, post_id: str, comment_id: str, comment_text: str):
        """Mark a commenter as scraped in database"""
        commenter_id = f"{username}_{post_id}_{comment_id}"
        
        try:
            with db_manager.get_cursor() as cursor:
                # Use datetime.now() for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO scraped_commenters 
                    (commenter_id, account_username, username, post_id, comment_id, comment_text, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (commenter_id, account_username) DO NOTHING
                """, (commenter_id, self.account_username, username, post_id, 
                      comment_id, comment_text[:1000], current_time))
                
            logger.debug(f"📝 [{self.account_username}] Marked HOT1 commenter {username} (comment: {comment_id[:8]}) as scraped in database")
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking HOT1 commenter scraped: {e}")
    
    def scrape_active_commenters(self, max_comment_age_minutes: int = 300, max_posts: int = 100) -> list:
        """
        Scrapes active commenters from community HOT feed
        Returns comments from the last 300 minutes (5 hours) instead of 60
        Returns the FIRST available commenter we can actually message
        """
        current_time = time.time()
        
        logger.info(f"🔍 [{self.account_username}] Scraping active commenters from HOT community feed (max {max_posts} posts, up to {max_comment_age_minutes//60} hours)")
        
        try:
            url = self.feed_url
            params = {
                'limit': max_posts,
                'raw_json': 1
            }
            
            logger.info(f"🔥 [{self.account_username}] Scraping HOT community feed (max {max_posts} posts)...")
            
            # Add initial delay to avoid rate limiting
            time.sleep(2)
            
            response = self.session.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code != 200:
                if response.status_code == 429:
                    logger.warning(f"⏳ [{self.account_username}] Rate limited, waiting 30 seconds")
                    time.sleep(30)
                    return []
                logger.error(f"❌ [{self.account_username}] Failed to scrape HOT community feed: {response.status_code}")
                return []
            
            data = response.json()
            posts = data['data']['children']
            
            total_posts = len(posts)
            logger.info(f"📊 [{self.account_username}] Found {total_posts} HOT posts in community feed")
            
            if total_posts == 0:
                logger.warning(f"⚠️ [{self.account_username}] No HOT posts found in community feed!")
                return []
            
            # Track statistics
            total_comments_scanned = 0
            recent_comments_found = 0
            posts_checked = 0
            commenters_skipped = {
                'too_old': 0,
                'already_scraped': 0,
                'bot_mod': 0,
                'no_recent_comments': 0,
                'already_processed': 0
            }
            
            for post_index, post in enumerate(posts):
                posts_checked += 1
                post_data = post['data']
                post_id = post_data['id']
                post_title = post_data['title'][:100]
                post_author = post_data['author']
                post_subreddit = post_data.get('subreddit', 'unknown')
                post_permalink = post_data.get('permalink', '')
                post_score = post_data.get('score', 0)
                post_created = post_data.get('created_utc', 0)
                
                # Skip very old posts (older than 24 hours)
                post_age_hours = (current_time - post_created) / 3600
                if post_age_hours > 24:
                    logger.debug(f"⏰ [{self.account_username}] Skipping old HOT post ({post_age_hours:.1f}h): r/{post_subreddit}")
                    continue
                
                # Skip AutoModerator posts
                if post_author in ['AutoModerator', '[deleted]', '[removed]']:
                    continue
                
                logger.debug(f"🔥 [{self.account_username}] Checking HOT post {post_index+1}/{total_posts}: r/{post_subreddit}")
                
                # Get comments for this post (with rate limiting)
                post_comments = self._scrape_post_comments(post_permalink)
                
                if not post_comments:
                    continue
                
                logger.debug(f"💬 [{self.account_username}] Found {len(post_comments)} comments on HOT post")
                total_comments_scanned += len(post_comments)
                
                # Process each comment
                for comment_index, comment in enumerate(post_comments[:30]):  # Check up to 30 comments per post
                    comment_author = comment['author']
                    comment_id = comment['id']
                    comment_body = comment['body']
                    comment_created_utc = comment['created_utc']
                    
                    # Skip bot/mod comments
                    if any(bot in comment_author.lower() for bot in ['auto', 'mod', 'bot', 'deleted', 'removed']):
                        commenters_skipped['bot_mod'] += 1
                        continue
                    
                    # Check comment age (comments from last 300 minutes = 5 hours)
                    comment_age_seconds = current_time - comment_created_utc
                    comment_age_minutes = comment_age_seconds / 60
                    
                    if comment_age_minutes > max_comment_age_minutes:
                        commenters_skipped['too_old'] += 1
                        logger.debug(f"⏰ [{self.account_username}] Skipping HOT comment from {comment_author}: {comment_age_minutes:.1f}m old (>5h)")
                        continue
                    
                    recent_comments_found += 1
                    
                    # Check if commenter already scraped
                    commenter_id = f"{comment_author}_{post_id}_{comment_id}"
                    if self.is_commenter_scraped(commenter_id):
                        commenters_skipped['already_scraped'] += 1
                        logger.debug(f"📭 [{self.account_username}] Already scraped HOT commenter: {comment_author}")
                        continue
                    
                    # Get user's recent comments to ensure they're active
                    recent_comments = self._get_user_recent_comments(comment_author, limit=2)
                    
                    if not recent_comments or len(recent_comments) == 0:
                        commenters_skipped['no_recent_comments'] += 1
                        logger.debug(f"📭 [{self.account_username}] No recent comments found for HOT commenter {comment_author}")
                        continue
                    
                    # CRITICAL FIX: Check if user is available BEFORE marking as eligible
                    # Check if commenter already processed globally or by another account
                    if not global_processed_manager.can_process_user(comment_author, self.account_username):
                        commenters_skipped['already_processed'] += 1
                        logger.debug(f"⏭️  [{self.account_username}] Skipping HOT {comment_author} - already processed globally or in progress")
                        continue
                    
                    # Try to reserve this user for processing
                    if not global_processed_manager.reserve_user_for_processing(comment_author, self.account_username):
                        commenters_skipped['already_processed'] += 1
                        logger.debug(f"⏭️  [{self.account_username}] HOT User {comment_author} was taken by another account just now")
                        continue
                    
                    # NOW we have a truly eligible commenter - we can message them!
                    commenter_info = {
                        'commenter_username': comment_author,
                        'post_id': post_id,
                        'comment_id': comment_id,
                        'post_title': post_title,
                        'post_subreddit': post_subreddit,
                        'post_author': post_author,
                        'comment_text': comment_body,
                        'comment_age_minutes': comment_age_minutes,
                        'comment_created_utc': comment_created_utc,
                        'recent_comments': recent_comments,
                        'scraper_id': f"{comment_author}_{post_id}_{comment_id}",
                        'is_hot_feed': True
                    }
                    
                    # Mark as scraped
                    self.mark_commenter_scraped(comment_author, post_id, comment_id, comment_body[:100])
                    
                    logger.info(f"✅ [{self.account_username}] Found AVAILABLE HOT commenter: {comment_author} - '{comment_body[:50]}...' ({comment_age_minutes:.1f}m ago, within 5h)")
                    logger.info(f"📊 [{self.account_username}] Stopping search after checking {posts_checked} HOT posts, {total_comments_scanned} comments")
                    logger.info(f"📊 HOT Skipped stats: {commenters_skipped}")
                    
                    # Return just this one commenter - we found what we need!
                    return [commenter_info]
            
            # Log summary if no available commenters found
            logger.info(f"📊 [{self.account_username}] No AVAILABLE HOT commenters found in {posts_checked} posts (5h window)")
            logger.info(f"   📝 HOT Posts checked: {posts_checked}")
            logger.info(f"   💬 Comments scanned: {total_comments_scanned}")
            logger.info(f"   ⏰ Recent comments (<{max_comment_age_minutes}m): {recent_comments_found}")
            logger.info(f"   🚫 Skipped commenters:")
            logger.info(f"     - Too old (>5h): {commenters_skipped['too_old']}")
            logger.info(f"     - Already scraped: {commenters_skipped['already_scraped']}")
            logger.info(f"     - Bot/Mod: {commenters_skipped['bot_mod']}")
            logger.info(f"     - No recent comments: {commenters_skipped['no_recent_comments']}")
            logger.info(f"     - Already processed globally: {commenters_skipped['already_processed']}")
            
            return []
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error scraping HOT community feed: {e}")
            return []
    
    def _scrape_post_comments(self, post_permalink: str) -> list:
        """Scrape comments from a specific post - WITH RATE LIMITING"""
        try:
            comments_url = f"https://www.reddit.com{post_permalink}.json"
            params = {
                'limit': 100,
                'raw_json': 1,
                'depth': 1,    # Only get top-level comments
                'sort': 'new'
            }
            
            # Add delay to avoid rate limiting
            time.sleep(1.5)
            
            response = self.session.get(comments_url, params=params, timeout=20, verify=False)
            
            if response.status_code != 200:
                if response.status_code == 429:
                    logger.warning(f"⏳ [{self.account_username}] Rate limited on HOT comments, waiting 30 seconds")
                    time.sleep(30)
                    return []
                logger.debug(f"⚠️ [{self.account_username}] Failed to scrape HOT comments: {response.status_code}")
                return []
            
            data = response.json()
            
            if len(data) < 2:
                return []
            
            comments_data = data[1]['data']['children']
            comments = []
            
            for comment in comments_data[:30]:  # Check up to 30 comments
                if comment['kind'] == 't1':
                    comment_data = comment['data']
                    
                    body = comment_data.get('body', '')
                    if body in ['[deleted]', '[removed]']:
                        continue
                    
                    if len(body.strip()) < 3:
                        continue
                    
                    comments.append({
                        'id': comment_data['id'],
                        'author': comment_data['author'],
                        'body': body,
                        'created_utc': comment_data['created_utc'],
                        'score': comment_data.get('score', 0)
                    })
            
            logger.debug(f"📄 [{self.account_username}] Extracted {len(comments)} HOT comments from post")
            return comments
            
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error scraping HOT post comments: {e}")
            return []
    
    def _get_user_recent_comments(self, username: str, limit: int = 2) -> list:
        """Get user's recent comments (for context in message generation)"""
        try:
            url = f"https://www.reddit.com/user/{username}/comments.json"
            params = {
                'limit': limit,
                'raw_json': 1
            }
            
            # Add delay to avoid rate limiting
            time.sleep(1.5)
            
            response = self.session.get(url, params=params, timeout=15, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                comments_data = data['data']['children']
                
                recent_comments = []
                for comment in comments_data:
                    if comment['kind'] == 't1':
                        comment_info = comment['data']
                        recent_comments.append({
                            'body': comment_info.get('body', ''),
                            'subreddit': comment_info.get('subreddit', 'Unknown'),
                            'created_utc': comment_info.get('created_utc', 0)
                        })
                
                return recent_comments
            else:
                if response.status_code == 429:
                    logger.debug(f"⏳ [{self.account_username}] Rate limited getting HOT user comments for {username}")
                    time.sleep(30)
                logger.debug(f"📭 [{self.account_username}] No recent comments found for HOT user {username}")
                return []
                
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error getting recent comments for HOT user {username}: {e}")
            return []
    
    def cleanup_old_scraped_commenters(self, max_age_days: int = 7):
        """Clean up old scraped commenters"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    DELETE FROM scraped_commenters 
                    WHERE account_username = %s 
                    AND scraped_at < NOW() - INTERVAL '%s days'
                """, (self.account_username, max_age_days))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"🧹 [{self.account_username}] Cleaned up {deleted_count} old HOT1 scraped commenters")
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error cleaning up HOT1 scraped commenters: {e}")

# ========== COMMUNITY FEED SCRAPER 4 (HOT FEED METHOD 5) ==========
class CommunityFeedScraperHot2:
    """Scrapes active commenters from the second community feed (all_nsfw_2) HOT feed"""
    
    def __init__(self, account_username: str, session: requests.Session):
        self.account_username = account_username
        self.session = session
        self.feed_url = "https://www.reddit.com/user/rileyxwood/m/all_nsfw_2/hot/.json"
        
        # Store scraped commenters to avoid duplicates
        self.scraped_commenters_file = Path(f"scraped_commenters_hot2_{account_username}.json")
        self.scraped_commenters = self._load_scraped_commenters()
        
        logger.info(f"🔥 [{self.account_username}] Second community HOT feed scraper initialized (5-hour search window)")
    
    def _load_scraped_commenters(self) -> set:
        """Load previously scraped commenters from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT commenter_id FROM scraped_commenters 
                    WHERE account_username = %s
                """, (self.account_username,))
                
                results = cursor.fetchall()
                commenters = {row['commenter_id'] for row in results}
                logger.info(f"📁 [{self.account_username}] Loaded {len(commenters)} previously scraped HOT2 commenters from database")
                return commenters
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error loading scraped HOT2 commenters: {e}")
            return set()
    
    def is_commenter_scraped(self, commenter_id: str) -> bool:
        """Check if a commenter has already been scraped in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM scraped_commenters 
                    WHERE commenter_id = %s AND account_username = %s
                    LIMIT 1
                """, (commenter_id, self.account_username))
                
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking scraped HOT2 commenter: {e}")
            return False
    
    def mark_commenter_scraped(self, username: str, post_id: str, comment_id: str, comment_text: str):
        """Mark a commenter as scraped in database"""
        commenter_id = f"{username}_{post_id}_{comment_id}"
        
        try:
            with db_manager.get_cursor() as cursor:
                # Use datetime.now() for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO scraped_commenters 
                    (commenter_id, account_username, username, post_id, comment_id, comment_text, scraped_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (commenter_id, account_username) DO NOTHING
                """, (commenter_id, self.account_username, username, post_id, 
                      comment_id, comment_text[:1000], current_time))
                
            logger.debug(f"📝 [{self.account_username}] Marked HOT2 commenter {username} (comment: {comment_id[:8]}) as scraped in database")
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking HOT2 commenter scraped: {e}")
    
    def scrape_active_commenters(self, max_comment_age_minutes: int = 300, max_posts: int = 100) -> list:
        """
        Scrapes active commenters from community HOT feed
        Returns comments from the last 300 minutes (5 hours) instead of 60
        Returns the FIRST available commenter we can actually message
        """
        current_time = time.time()
        
        logger.info(f"🔍 [{self.account_username}] Scraping active commenters from HOT community feed 2 (max {max_posts} posts, up to {max_comment_age_minutes//60} hours)")
        
        try:
            url = self.feed_url
            params = {
                'limit': max_posts,
                'raw_json': 1
            }
            
            logger.info(f"🔥 [{self.account_username}] Scraping HOT community feed 2 (max {max_posts} posts)...")
            
            # Add initial delay to avoid rate limiting
            time.sleep(2)
            
            response = self.session.get(url, params=params, timeout=30, verify=False)
            
            if response.status_code != 200:
                if response.status_code == 429:
                    logger.warning(f"⏳ [{self.account_username}] Rate limited, waiting 30 seconds")
                    time.sleep(30)
                    return []
                logger.error(f"❌ [{self.account_username}] Failed to scrape HOT community feed 2: {response.status_code}")
                return []
            
            data = response.json()
            posts = data['data']['children']
            
            total_posts = len(posts)
            logger.info(f"📊 [{self.account_username}] Found {total_posts} HOT posts in community feed 2")
            
            if total_posts == 0:
                logger.warning(f"⚠️ [{self.account_username}] No HOT posts found in community feed 2!")
                return []
            
            # Track statistics
            total_comments_scanned = 0
            recent_comments_found = 0
            posts_checked = 0
            commenters_skipped = {
                'too_old': 0,
                'already_scraped': 0,
                'bot_mod': 0,
                'no_recent_comments': 0,
                'already_processed': 0
            }
            
            for post_index, post in enumerate(posts):
                posts_checked += 1
                post_data = post['data']
                post_id = post_data['id']
                post_title = post_data['title'][:100]
                post_author = post_data['author']
                post_subreddit = post_data.get('subreddit', 'unknown')
                post_permalink = post_data.get('permalink', '')
                post_score = post_data.get('score', 0)
                post_created = post_data.get('created_utc', 0)
                
                # Skip very old posts (older than 24 hours)
                post_age_hours = (current_time - post_created) / 3600
                if post_age_hours > 24:
                    logger.debug(f"⏰ [{self.account_username}] Skipping old HOT post 2 ({post_age_hours:.1f}h): r/{post_subreddit}")
                    continue
                
                # Skip AutoModerator posts
                if post_author in ['AutoModerator', '[deleted]', '[removed]']:
                    continue
                
                logger.debug(f"🔥 [{self.account_username}] Checking HOT post 2 {post_index+1}/{total_posts}: r/{post_subreddit}")
                
                # Get comments for this post (with rate limiting)
                post_comments = self._scrape_post_comments(post_permalink)
                
                if not post_comments:
                    continue
                
                logger.debug(f"💬 [{self.account_username}] Found {len(post_comments)} comments on HOT post 2")
                total_comments_scanned += len(post_comments)
                
                # Process each comment
                for comment_index, comment in enumerate(post_comments[:30]):  # Check up to 30 comments per post
                    comment_author = comment['author']
                    comment_id = comment['id']
                    comment_body = comment['body']
                    comment_created_utc = comment['created_utc']
                    
                    # Skip bot/mod comments
                    if any(bot in comment_author.lower() for bot in ['auto', 'mod', 'bot', 'deleted', 'removed']):
                        commenters_skipped['bot_mod'] += 1
                        continue
                    
                    # Check comment age (comments from last 300 minutes = 5 hours)
                    comment_age_seconds = current_time - comment_created_utc
                    comment_age_minutes = comment_age_seconds / 60
                    
                    if comment_age_minutes > max_comment_age_minutes:
                        commenters_skipped['too_old'] += 1
                        logger.debug(f"⏰ [{self.account_username}] Skipping HOT comment 2 from {comment_author}: {comment_age_minutes:.1f}m old (>5h)")
                        continue
                    
                    recent_comments_found += 1
                    
                    # Check if commenter already scraped
                    commenter_id = f"{comment_author}_{post_id}_{comment_id}"
                    if self.is_commenter_scraped(commenter_id):
                        commenters_skipped['already_scraped'] += 1
                        logger.debug(f"📭 [{self.account_username}] Already scraped HOT commenter 2: {comment_author}")
                        continue
                    
                    # Get user's recent comments to ensure they're active
                    recent_comments = self._get_user_recent_comments(comment_author, limit=2)
                    
                    if not recent_comments or len(recent_comments) == 0:
                        commenters_skipped['no_recent_comments'] += 1
                        logger.debug(f"📭 [{self.account_username}] No recent comments found for HOT commenter 2 {comment_author}")
                        continue
                    
                    # CRITICAL FIX: Check if user is available BEFORE marking as eligible
                    # Check if commenter already processed globally or by another account
                    if not global_processed_manager.can_process_user(comment_author, self.account_username):
                        commenters_skipped['already_processed'] += 1
                        logger.debug(f"⏭️  [{self.account_username}] Skipping HOT 2 {comment_author} - already processed globally or in progress")
                        continue
                    
                    # Try to reserve this user for processing
                    if not global_processed_manager.reserve_user_for_processing(comment_author, self.account_username):
                        commenters_skipped['already_processed'] += 1
                        logger.debug(f"⏭️  [{self.account_username}] HOT 2 User {comment_author} was taken by another account just now")
                        continue
                    
                    # NOW we have a truly eligible commenter - we can message them!
                    commenter_info = {
                        'commenter_username': comment_author,
                        'post_id': post_id,
                        'comment_id': comment_id,
                        'post_title': post_title,
                        'post_subreddit': post_subreddit,
                        'post_author': post_author,
                        'comment_text': comment_body,
                        'comment_age_minutes': comment_age_minutes,
                        'comment_created_utc': comment_created_utc,
                        'recent_comments': recent_comments,
                        'scraper_id': f"{comment_author}_{post_id}_{comment_id}",
                        'is_hot_feed': True
                    }
                    
                    # Mark as scraped
                    self.mark_commenter_scraped(comment_author, post_id, comment_id, comment_body[:100])
                    
                    logger.info(f"✅ [{self.account_username}] Found AVAILABLE HOT commenter 2: {comment_author} - '{comment_body[:50]}...' ({comment_age_minutes:.1f}m ago, within 5h)")
                    logger.info(f"📊 [{self.account_username}] Stopping search after checking {posts_checked} HOT posts, {total_comments_scanned} comments")
                    logger.info(f"📊 HOT 2 Skipped stats: {commenters_skipped}")
                    
                    # Return just this one commenter - we found what we need!
                    return [commenter_info]
            
            # Log summary if no available commenters found
            logger.info(f"📊 [{self.account_username}] No AVAILABLE HOT commenters found in {posts_checked} posts (5h window)")
            logger.info(f"   📝 HOT Posts checked: {posts_checked}")
            logger.info(f"   💬 Comments scanned: {total_comments_scanned}")
            logger.info(f"   ⏰ Recent comments (<{max_comment_age_minutes}m): {recent_comments_found}")
            logger.info(f"   🚫 Skipped commenters:")
            logger.info(f"     - Too old (>5h): {commenters_skipped['too_old']}")
            logger.info(f"     - Already scraped: {commenters_skipped['already_scraped']}")
            logger.info(f"     - Bot/Mod: {commenters_skipped['bot_mod']}")
            logger.info(f"     - No recent comments: {commenters_skipped['no_recent_comments']}")
            logger.info(f"     - Already processed globally: {commenters_skipped['already_processed']}")
            
            return []
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error scraping HOT community feed 2: {e}")
            return []
    
    def _scrape_post_comments(self, post_permalink: str) -> list:
        """Scrape comments from a specific post - WITH RATE LIMITING"""
        try:
            comments_url = f"https://www.reddit.com{post_permalink}.json"
            params = {
                'limit': 100,
                'raw_json': 1,
                'depth': 1,    # Only get top-level comments
                'sort': 'new'
            }
            
            # Add delay to avoid rate limiting
            time.sleep(1.5)
            
            response = self.session.get(comments_url, params=params, timeout=20, verify=False)
            
            if response.status_code != 200:
                if response.status_code == 429:
                    logger.warning(f"⏳ [{self.account_username}] Rate limited on HOT comments 2, waiting 30 seconds")
                    time.sleep(30)
                    return []
                logger.debug(f"⚠️ [{self.account_username}] Failed to scrape HOT comments 2: {response.status_code}")
                return []
            
            data = response.json()
            
            if len(data) < 2:
                return []
            
            comments_data = data[1]['data']['children']
            comments = []
            
            for comment in comments_data[:30]:  # Check up to 30 comments
                if comment['kind'] == 't1':
                    comment_data = comment['data']
                    
                    body = comment_data.get('body', '')
                    if body in ['[deleted]', '[removed]']:
                        continue
                    
                    if len(body.strip()) < 3:
                        continue
                    
                    comments.append({
                        'id': comment_data['id'],
                        'author': comment_data['author'],
                        'body': body,
                        'created_utc': comment_data['created_utc'],
                        'score': comment_data.get('score', 0)
                    })
            
            logger.debug(f"📄 [{self.account_username}] Extracted {len(comments)} HOT comments from post 2")
            return comments
            
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error scraping HOT post comments 2: {e}")
            return []
    
    def _get_user_recent_comments(self, username: str, limit: int = 2) -> list:
        """Get user's recent comments (for context in message generation)"""
        try:
            url = f"https://www.reddit.com/user/{username}/comments.json"
            params = {
                'limit': limit,
                'raw_json': 1
            }
            
            # Add delay to avoid rate limiting
            time.sleep(1.5)
            
            response = self.session.get(url, params=params, timeout=15, verify=False)
            
            if response.status_code == 200:
                data = response.json()
                comments_data = data['data']['children']
                
                recent_comments = []
                for comment in comments_data:
                    if comment['kind'] == 't1':
                        comment_info = comment['data']
                        recent_comments.append({
                            'body': comment_info.get('body', ''),
                            'subreddit': comment_info.get('subreddit', 'Unknown'),
                            'created_utc': comment_info.get('created_utc', 0)
                        })
                
                return recent_comments
            else:
                if response.status_code == 429:
                    logger.debug(f"⏳ [{self.account_username}] Rate limited getting HOT user comments 2 for {username}")
                    time.sleep(30)
                logger.debug(f"📭 [{self.account_username}] No recent comments found for HOT user 2 {username}")
                return []
                
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error getting recent comments for HOT user 2 {username}: {e}")
            return []
    
    def cleanup_old_scraped_commenters(self, max_age_days: int = 7):
        """Clean up old scraped commenters"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    DELETE FROM scraped_commenters 
                    WHERE account_username = %s 
                    AND scraped_at < NOW() - INTERVAL '%s days'
                """, (self.account_username, max_age_days))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"🧹 [{self.account_username}] Cleaned up {deleted_count} old HOT2 scraped commenters")
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error cleaning up HOT2 scraped commenters: {e}")
        
# ========== DATABASE ROOM METADATA MANAGER ==========
class RoomMetadataManager:
    """Passively tracks room metadata based on sync activity in database"""
    
    def __init__(self, account_username: str):
        self.account_username = account_username
        logger.info(f"📊 Database Room Metadata Manager initialized for {account_username}")
    
    def update_room_metadata_from_sync(self, room_id: str, events: list):
        """Update metadata for a room based on sync events in database"""
        current_time = datetime.now()  # Use datetime object instead of milliseconds
        
        if not events:
            return
        
        # Find the latest message timestamp
        message_events = [e for e in events if e.get('type') == 'm.room.message']
        if not message_events:
            return
        
        latest_event = max(message_events, key=lambda x: x.get('origin_server_ts', 0))
        last_activity_ms = latest_event.get('origin_server_ts', 0)
        
        # Convert milliseconds to datetime for PostgreSQL timestamp
        # If last_activity_ms is 0, use current time
        if last_activity_ms > 0:
            # Convert milliseconds to seconds and create datetime
            last_activity_dt = datetime.fromtimestamp(last_activity_ms / 1000)
        else:
            last_activity_dt = current_time
        
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO room_metadata 
                    (room_id, account_username, last_checked, last_activity, total_messages, check_count, status)
                    VALUES (%s, %s, %s, %s, %s, 1, 'active')
                    ON CONFLICT (room_id, account_username) DO UPDATE SET
                    last_checked = EXCLUDED.last_checked,
                    last_activity = EXCLUDED.last_activity,
                    total_messages = room_metadata.total_messages + EXCLUDED.total_messages,
                    check_count = room_metadata.check_count + 1,
                    status = 'active'
                """, (room_id, self.account_username, current_time, 
                      last_activity_dt, len(message_events)))
            
            logger.debug(f"📊 [{self.account_username}] Updated metadata for room {room_id[:8]} in database: {len(message_events)} messages")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error updating room metadata: {e}")
            
    def cleanup_old_rooms(self, max_age_days: int = 30):
        """Remove metadata for rooms older than max_age_days from database"""
        try:
            with db_manager.get_cursor() as cursor:
                # Convert days to milliseconds for comparison
                max_age_ms = max_age_days * 24 * 60 * 60 * 1000
                current_time_ms = int(time.time() * 1000)
                cutoff_ms = current_time_ms - max_age_ms
                
                cursor.execute("""
                    DELETE FROM room_metadata 
                    WHERE account_username = %s 
                    AND last_activity < %s
                """, (self.account_username, cutoff_ms))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"🧹 [{self.account_username}] Cleaned up {deleted_count} old rooms from metadata database")
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error cleaning up old rooms: {e}")
    
    def get_room_status(self, room_id: str) -> str:
        """Get status of a room from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT status FROM room_metadata 
                    WHERE room_id = %s AND account_username = %s
                    LIMIT 1
                """, (room_id, self.account_username))
                
                result = cursor.fetchone()
                return result['status'] if result else 'unknown'
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting room status: {e}")
            return 'unknown'
    
    def cleanup_old_rooms(self, max_age_days: int = 30):
        """Remove metadata for rooms older than max_age_days from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    DELETE FROM room_metadata 
                    WHERE account_username = %s 
                    AND last_activity < NOW() - INTERVAL '%s days'
                """, (self.account_username, max_age_days))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"🧹 [{self.account_username}] Cleaned up {deleted_count} old rooms from metadata database")
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error cleaning up old rooms: {e}")

class ImageAnalyzer:
    """Analyzes images using the Joy Caption AI model"""
    
    def __init__(self):
        self.client = None
        self.hf_token = "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"
        self.direct_session = requests.Session()
        self.initialize_client()
    
    def initialize_client(self):
        """Initialize the image analysis client with authentication"""
        try:
            logger.info("🔐 Logging into Hugging Face...")
            login(token=self.hf_token)
            logger.info("✅ Successfully logged into Hugging Face")
            
            os.environ['HF_TOKEN'] = self.hf_token
            logger.info("✅ Set HF_TOKEN environment variable")
            
            self.client = Client("fancyfeast/joy-caption-alpha-two")
            logger.info("✅ Connected to Hugging Face Image Analysis API with authentication")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to connect to Image Analysis API: {e}")
            return False
    
    def analyze_image(self, image_path: str) -> str:
        """Analyze an image and return description"""
        if not self.client:
            logger.error("❌ Image analyzer not initialized - authentication failed")
            return "I see you sent an image, but I'm having trouble analyzing it right now."
        
        try:
            logger.info(f"🖼️ Analyzing image: {image_path}")
            
            result = self.client.predict(
                input_image=handle_file(image_path),
                caption_type="Descriptive",
                caption_length="long", 
                extra_options=[],
                name_input="person",
                custom_prompt="",
                api_name="/stream_chat"
            )
            
            if result and len(result) >= 2:
                caption = result[1]
                logger.info(f"📝 Image analysis result: {caption[:100]}...")
                return caption
            else:
                return "I see an image but can't quite describe what's in it."
                
        except Exception as e:
            logger.error(f"❌ Image analysis error: {e}")
            return "I see you sent an image, but I'm having trouble analyzing it right now."

# ========== DATABASE PERSISTENT STATE MANAGER ==========
class PersistentStateManager:
    """Manages persistent state for message processing in database"""
    
    def __init__(self, account_username: str):
        self.account_username = account_username
        logger.info(f"📁 Database Persistent State Manager initialized for {account_username}")
    
    def is_message_processed(self, message_id: str) -> bool:
        """Check if a message has already been processed in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM processed_messages 
                    WHERE message_id = %s AND account_username = %s
                    LIMIT 1
                """, (message_id, self.account_username))
                
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking processed message: {e}")
            return False

    def save_state(self):
        """Save state to database - This is a dummy method for compatibility"""
        # This method doesn't need to do anything since state is saved incrementally
        # It's only here because mark_message_as_processed_and_update_cursor() calls it
        logger.debug(f"💾 [{self.account_username}] save_state() called (no-op for database version)")
        pass

    def mark_message_processed(self, message_id: str):
        """Mark a message as processed in database"""
        try:
            with db_manager.get_cursor() as cursor:
                # Use datetime.now() for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO processed_messages (message_id, account_username, processed_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (message_id, account_username) DO NOTHING
                """, (message_id, self.account_username, current_time))
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking message processed: {e}")

    def mark_image_analyzed(self, image_identifier: str, analysis_result: str):
        """Mark an image as analyzed and cache the result in database"""
        try:
            with db_manager.get_cursor() as cursor:
                # Use datetime.now() for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO analyzed_images (image_identifier, account_username, analysis_result, analyzed_at)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (image_identifier, account_username) DO UPDATE SET
                    analysis_result = EXCLUDED.analysis_result,
                    analyzed_at = EXCLUDED.analyzed_at
                """, (image_identifier, self.account_username, analysis_result, current_time))
                
            logger.debug(f"💾 [{self.account_username}] Cached image analysis for: {image_identifier[:50]}... in database")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking image analyzed: {e}")

    def save_conversation_state(self, room_id: str, history: list, message_count: int, stage: str):
        """Save conversation state for a room to database"""
        try:
            current_time = datetime.now()  # Use datetime object
            
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO conversation_states (room_id, account_username, history, message_count, stage, total_messages, last_updated)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (room_id, account_username) DO UPDATE SET
                    history = EXCLUDED.history,
                    message_count = EXCLUDED.message_count,
                    stage = EXCLUDED.stage,
                    total_messages = EXCLUDED.total_messages,
                    last_updated = EXCLUDED.last_updated
                """, (
                    room_id,
                    self.account_username,
                    json.dumps(history[-10:] if len(history) > 10 else history),
                    message_count,
                    stage,
                    len(history),
                    current_time  # Pass datetime object
                ))
                
            logger.debug(f"💾 [{self.account_username}] Saved conversation state for room {room_id[:8]} to database: {message_count} messages, stage: {stage}")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error saving conversation state: {e}")
    
    def get_room_cursor(self, room_id: str) -> int:
        """Get the last processed timestamp for a room from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT last_cursor FROM room_cursors 
                    WHERE room_id = %s AND account_username = %s
                    LIMIT 1
                """, (room_id, self.account_username))
                
                result = cursor.fetchone()
                return result['last_cursor'] if result else 0
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting room cursor: {e}")
            return 0
    
    def update_room_cursor(self, room_id: str, timestamp: int):
        """Update the cursor for a room in database"""
        try:
            # Ensure timestamp is in milliseconds
            timestamp_ms = timestamp
            if timestamp_ms < 1000000000000:  # If it's in seconds, convert to milliseconds
                timestamp_ms = timestamp * 1000
                
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO room_cursors (room_id, account_username, last_cursor, last_updated)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (room_id, account_username) DO UPDATE SET
                    last_cursor = GREATEST(room_cursors.last_cursor, EXCLUDED.last_cursor),
                    last_updated = %s
                """, (room_id, self.account_username, timestamp_ms, 
                      datetime.now(),  # Use datetime for initial insert
                      datetime.now()))  # Use datetime for update
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error updating room cursor: {e}")

    def update_last_reply_timestamp(self, room_id: str, timestamp: int):
        """Update the timestamp of our last reply in this conversation in database"""
        try:
            # Ensure timestamp is in milliseconds
            timestamp_ms = timestamp
            if timestamp_ms < 1000000000000:  # If it's in seconds, convert to milliseconds
                timestamp_ms = timestamp * 1000
                
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO last_reply_timestamps (room_id, account_username, timestamp, last_updated)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (room_id, account_username) DO UPDATE SET
                    timestamp = GREATEST(last_reply_timestamps.timestamp, EXCLUDED.timestamp),
                    last_updated = %s
                """, (room_id, self.account_username, timestamp_ms, 
                      datetime.now(),  # Use datetime for initial insert
                      datetime.now()))  # Use datetime for update
                
            logger.debug(f"📝 [{self.account_username}] Updated last reply timestamp for room {room_id[:8]}: {timestamp_ms}")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error updating last reply timestamp: {e}")

    def get_last_reply_timestamp(self, room_id: str) -> int:
        """Get the timestamp of our last reply in this conversation from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT timestamp FROM last_reply_timestamps 
                    WHERE room_id = %s AND account_username = %s
                    LIMIT 1
                """, (room_id, self.account_username))
                
                result = cursor.fetchone()
                return result['timestamp'] if result else 0
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting last reply timestamp: {e}")
            return 0
    
    def get_last_reply_timestamp(self, room_id: str) -> int:
        """Get the timestamp of our last reply in this conversation from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT timestamp FROM last_reply_timestamps 
                    WHERE room_id = %s AND account_username = %s
                    LIMIT 1
                """, (room_id, self.account_username))
                
                result = cursor.fetchone()
                return result['timestamp'] if result else 0
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting last reply timestamp: {e}")
            return 0
    
    def update_last_reply_timestamp(self, room_id: str, timestamp: int):
        """Update the timestamp of our last reply in this conversation in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO last_reply_timestamps (room_id, account_username, timestamp)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (room_id, account_username) DO UPDATE SET
                    timestamp = GREATEST(last_reply_timestamps.timestamp, EXCLUDED.timestamp),
                    last_updated = NOW()
                """, (room_id, self.account_username, timestamp))
                
            logger.debug(f"📝 [{self.account_username}] Updated last reply timestamp for room {room_id[:8]}: {timestamp}")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error updating last reply timestamp: {e}")
    
    def should_process_message(self, room_id: str, message_id: str, timestamp: int, our_user_id: str) -> bool:
        """
        Determine if a message should be processed.
        Returns False if:
        1. Message already processed
        2. Message is older than our LAST REPLY in this conversation
        """
        if self.is_message_processed(message_id):
            return False
        
        last_reply_time = self.get_last_reply_timestamp(room_id)
        
        if timestamp <= last_reply_time:
            logger.debug(f"📄 [{self.account_username}] Skipping message {message_id[:8]} for room {room_id[:8]}: "
                        f"timestamp {timestamp} <= last reply {last_reply_time}")
            return False
        
        return True
    
    def is_image_analyzed(self, image_identifier: str) -> bool:
        """Check if an image has already been analyzed in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM analyzed_images 
                    WHERE image_identifier = %s AND account_username = %s
                    LIMIT 1
                """, (image_identifier, self.account_username))
                
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking analyzed image: {e}")
            return False
    
    def get_image_analysis(self, image_identifier: str) -> str:
        """Get cached image analysis result from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT analysis_result FROM analyzed_images 
                    WHERE image_identifier = %s AND account_username = %s
                    LIMIT 1
                """, (image_identifier, self.account_username))
                
                result = cursor.fetchone()
                return result['analysis_result'] if result else None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting image analysis: {e}")
            return None
    
    def mark_image_analyzed(self, image_identifier: str, analysis_result: str):
        """Mark an image as analyzed and cache the result in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO analyzed_images (image_identifier, account_username, analysis_result)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (image_identifier, account_username) DO UPDATE SET
                    analysis_result = EXCLUDED.analysis_result,
                    analyzed_at = NOW()
                """, (image_identifier, self.account_username, analysis_result))
                
            logger.debug(f"💾 [{self.account_username}] Cached image analysis for: {image_identifier[:50]}... in database")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking image analyzed: {e}")
    
    def save_conversation_state(self, room_id: str, history: list, message_count: int, stage: str):
        """Save conversation state for a room to database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO conversation_states (room_id, account_username, history, message_count, stage, total_messages)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (room_id, account_username) DO UPDATE SET
                    history = EXCLUDED.history,
                    message_count = EXCLUDED.message_count,
                    stage = EXCLUDED.stage,
                    total_messages = EXCLUDED.total_messages,
                    last_updated = NOW()
                """, (
                    room_id,
                    self.account_username,
                    json.dumps(history[-10:] if len(history) > 10 else history),
                    message_count,
                    stage,
                    len(history)
                ))
                
            logger.debug(f"💾 [{self.account_username}] Saved conversation state for room {room_id[:8]} to database: {message_count} messages, stage: {stage}")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error saving conversation state: {e}")
    
    def load_conversation_state(self, room_id: str):
        """Load conversation state for a room from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT history, message_count, stage, total_messages 
                    FROM conversation_states 
                    WHERE room_id = %s AND account_username = %s
                    LIMIT 1
                """, (room_id, self.account_username))
                
                result = cursor.fetchone()
                
                if result:
                    # Handle different possible return types for history
                    history_data = result['history']
                    if isinstance(history_data, str):
                        # It's a JSON string, parse it
                        history = json.loads(history_data) if history_data else []
                    elif isinstance(history_data, list):
                        # It's already a list (direct from database)
                        history = history_data
                    elif history_data is None:
                        # It's None
                        history = []
                    else:
                        # Try to convert to string and parse
                        try:
                            history = json.loads(str(history_data))
                        except:
                            logger.error(f"❌ [{self.account_username}] Cannot parse history data type: {type(history_data)}")
                            history = []
                    
                    state = {
                        'history': history,
                        'message_count': result['message_count'],
                        'stage': result['stage'],
                        'last_updated': None,
                        'total_messages': result['total_messages']
                    }
                    
                    logger.debug(f"📁 [{self.account_username}] Loaded conversation state from database for room {room_id[:8]}: {state['message_count']} messages, stage: {state['stage']}")
                    return state
        
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error loading conversation state: {e}")
        
        # Default state if not found
        return {
            'history': [],
            'message_count': 0,
            'stage': 'intro',
            'last_updated': None,
            'total_messages': 0
        }
    
    def cleanup_old_messages(self, max_age_days: int = 7):
        """Clean up old processed messages from database to prevent unbounded growth"""
        try:
            with db_manager.get_cursor() as cursor:
                # Convert days to milliseconds
                max_age_ms = max_age_days * 24 * 60 * 60 * 1000
                current_time_ms = int(time.time() * 1000)
                cutoff_ms = current_time_ms - max_age_ms
                
                # Keep only last 10,000 messages per account
                cursor.execute("""
                    WITH ranked_messages AS (
                        SELECT id, ROW_NUMBER() OVER (ORDER BY processed_at DESC) as rn
                        FROM processed_messages 
                        WHERE account_username = %s
                        AND processed_at > %s
                    )
                    DELETE FROM processed_messages 
                    WHERE id IN (
                        SELECT id FROM ranked_messages WHERE rn > 10000
                    )
                """, (self.account_username, cutoff_ms))
                
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(f"🧹 [{self.account_username}] Cleaned up {deleted_count} old processed messages from database")
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error cleaning up old messages: {e}")

class ChatBotConfig:
    def __init__(self):
        self.config_files = {
            'main': 'Main.txt',
            'chatting_style': 'ChattingStyle.txt', 
            'background': 'Background.txt',
            'onlyfans': 'Onlyfans.txt',
            'phase1': 'Phase1.txt',
            'phase2': 'Phase2.txt',
            'phase3': 'Phase3.txt',
            'objections': 'Objections.txt'
        }
        self.load_all_configs()
        self.objection_responses = self._parse_objections()
    
    def load_all_configs(self):
        """Load all configuration from text files"""
        self.configs = {}
        for key, filename in self.config_files.items():
            try:
                if os.path.exists(filename):
                    with open(filename, 'r', encoding='utf-8') as f:
                        self.configs[key] = f.read().strip()
                    logger.info(f"📁 Loaded {filename}")
                else:
                    self.configs[key] = ""
                    with open(filename, 'w', encoding='utf-8') as f:
                        f.write("")
                    logger.info(f"📁 Created empty {filename} - please fill it")
            except Exception as e:
                logger.error(f"❌ Error loading {filename}: {e}")
                self.configs[key] = ""
    
    def get(self, key):
        """Get configuration value"""
        return self.configs.get(key, "")
    
    def _parse_objections(self):
        """Parse objections file into a dictionary of triggers and responses"""
        objections_text = self.get('objections')
        objection_responses = {}
        
        if not objections_text:
            return objection_responses
            
        lines = objections_text.split('\n')
        for line in lines:
            line = line.strip()
            if line and ':' in line:
                trigger, response = line.split(':', 1)
                objection_responses[trigger.strip().lower()] = response.strip()
        
        logger.info(f"🛡️  Loaded {len(objection_responses)} objection responses")
        return objection_responses
    
    def get_objection_response(self, user_message: str) -> str:
        """Check if user message matches any objection triggers and return appropriate response"""
        if not self.objection_responses:
            return None
            
        user_message_lower = user_message.lower()
        
        for trigger, response in self.objection_responses.items():
            if trigger in user_message_lower:
                logger.info(f"🛡️  Objection detected: '{trigger}' -> responding with objection handler")
                return response
        
        return None

# ========== DATABASE IMAGE URL CACHE ==========
class ImageURLCache:
    """Manages cached MXC URLs for images in database"""
    
    def __init__(self, account_username: str):
        self.account_username = account_username
        logger.info(f"🖼️ [{self.account_username}] Database Image URL Cache initialized")
    
    def _load_cache(self) -> dict:
        """Load cached URLs from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT image_type, image_filename, mxc_url 
                    FROM image_cache 
                    WHERE account_username = %s
                """, (self.account_username,))
                
                results = cursor.fetchall()
                cache = {}
                for row in results:
                    cache_key = f"{row['image_type']}/{row['image_filename']}"
                    cache[cache_key] = row['mxc_url']
                
                logger.info(f"📁 [{self.account_username}] Loaded {len(cache)} cached image URLs from database")
                return cache
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error loading image cache: {e}")
            return {}
    
    def save_cache(self):
        """Save cache to database (not needed - each entry saved individually)"""
        pass
    
    def get_cached_url(self, image_type: str, image_filename: str) -> str:
        """Get cached MXC URL for an image from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT mxc_url FROM image_cache 
                    WHERE account_username = %s AND image_type = %s AND image_filename = %s
                    LIMIT 1
                """, (self.account_username, image_type, image_filename))
                
                result = cursor.fetchone()
                return result['mxc_url'] if result else None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting cached URL: {e}")
            return None
    
    def cache_url(self, image_type: str, image_filename: str, mxc_url: str):
        """Cache a new MXC URL in database"""
        try:
            with db_manager.get_cursor() as cursor:
                # Use datetime.now() for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO image_cache (account_username, image_type, image_filename, mxc_url, cached_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (account_username, image_type, image_filename) DO UPDATE SET
                    mxc_url = EXCLUDED.mxc_url,
                    cached_at = EXCLUDED.cached_at
                """, (self.account_username, image_type, image_filename, mxc_url, current_time))
                
            logger.info(f"💾 [{self.account_username}] Cached URL for {image_type}/{image_filename} in database: {mxc_url[:50]}...")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error caching image URL: {e}")
    
    def get_all_cached_for_type(self, image_type: str) -> dict:
        """Get all cached URLs for a specific image type from database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT image_filename, mxc_url FROM image_cache 
                    WHERE account_username = %s AND image_type = %s
                """, (self.account_username, image_type))
                
                results = cursor.fetchall()
                return {row['image_filename']: row['mxc_url'] for row in results}
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting cached URLs for type {image_type}: {e}")
            return {}
    
    def get_cached_filename_for_type(self, image_type: str) -> str:
        """Get a random cached image filename for a type from database"""
        cached_images = self.get_all_cached_for_type(image_type)
        if cached_images:
            return random.choice(list(cached_images.keys()))
        return None

# ========== UPDATED IMAGEMANAGER CLASS WITH FOLDER SEQUENCING ==========
class ImageManager:
    def __init__(self, account_username: str):
        self.account_username = account_username
        self.images_base = Path("images")
        self.sent_images = {}
        self.folder_usage = {}  # NEW: Track which folders have been used per room: {room_id: {image_type: [used_folders]}}
        self.image_folders = {
            'selfie': ['chongalong221'],
            'feet': ['feet', 'foot', 'toes', 'soles'],
            'ass': ['ass', 'butt', 'booty', 'bum', 'rear', 'backside'],
            'tits': ['tits', 'boobs', 'breasts', 'chest', 'cleavage', 'bust'],
            'random': []  # Add random folder for Phase 2
        }
        self.url_cache = ImageURLCache(account_username)
        self.setup_image_folders()
    
    def setup_image_folders(self):
        """Create image folders if they don't exist"""
        self.images_base.mkdir(exist_ok=True)
        for folder in self.image_folders.keys():
            (self.images_base / folder).mkdir(exist_ok=True)
        logger.info(f"📁 [{self.account_username}] Image folders setup complete")
        
    def _is_image_sent_to_room(self, room_id: str, image_filename: str) -> bool:
        """Check if an image has already been sent to a room (database version)"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM sent_images 
                    WHERE account_username = %s 
                    AND room_id = %s 
                    AND image_filename = %s
                    LIMIT 1
                """, (self.account_username, room_id, image_filename))
                
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking if image sent to room: {e}")
            return False
            
    def _mark_image_sent_to_room(self, room_id: str, image_filename: str, image_type: str, folder_number: int = None, user_identifier: str = None):
        """Mark an image as sent to a room in database"""
        try:
            # If no user_identifier provided, try to extract from room state
            if not user_identifier:
                user_identifier = room_id  # Fallback to room_id if we can't get user
            
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    INSERT INTO sent_images 
                    (account_username, room_id, user_identifier, image_type, image_filename, folder_number, sent_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (account_username, room_id, image_filename) DO NOTHING
                """, (self.account_username, room_id, user_identifier, image_type, image_filename, folder_number))
                
            logger.debug(f"📝 [{self.account_username}] Marked image {image_filename} as sent to room {room_id[:8]} in database")
            
            # Also update in-memory cache for faster access
            if room_id not in self.sent_images:
                self.sent_images[room_id] = []
            
            # Add to in-memory list (just filename for comparison)
            if image_filename not in [os.path.basename(sent) for sent in self.sent_images[room_id]]:
                self.sent_images[room_id].append(image_filename)
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking image sent to room: {e}")
    
    def detect_image_request(self, message: str) -> str:
        """Detect if the message is requesting a specific type of image"""
        message_lower = message.lower()
        
        common_words = {'see', 'show', 'your', 'you', 'me', 'let', 'want', 'look', 'picture', 'photo'}
        message_words = set(message_lower.split())
        filtered_words = message_words - common_words
        filtered_message = ' '.join(filtered_words)
        
        priority_triggers = ['tits', 'boobs', 'breasts', 'ass', 'butt', 'booty', 'feet', 'foot', 'toes']
        
        for trigger in priority_triggers:
            if trigger in filtered_message:
                if trigger in ['tits', 'boobs', 'breasts']:
                    return 'tits'
                elif trigger in ['ass', 'butt', 'booty', 'bum', 'rear', 'backside']:
                    return 'ass'
                elif trigger in ['feet', 'foot', 'toes', 'soles']:
                    return 'feet'
        
        selfie_triggers = ['checkaboolang332']
        for trigger in selfie_triggers:
            if trigger in message_lower:
                return 'selfie'
        
        detected_types = []
        for folder, triggers in self.image_folders.items():
            for trigger in triggers:
                if trigger in message_lower:
                    detected_types.append(folder)
        
        if detected_types:
            if 'tits' in detected_types:
                return 'tits'
            elif 'ass' in detected_types:
                return 'ass'
            elif 'feet' in detected_types:
                return 'feet'
            elif 'selfie' in detected_types:
                return 'selfie'
        
        return None
    
    def _get_numbered_folders(self, image_type: str):
        """Get all numbered folders for an image type in sequential order"""
        type_folder = self.images_base / image_type
        if not type_folder.exists():
            return []
        
        # Find all numbered subfolders (1, 2, 3, etc.)
        numbered_folders = []
        for item in type_folder.iterdir():
            if item.is_dir() and item.name.isdigit():
                numbered_folders.append(item)
        
        # Sort by folder number
        numbered_folders.sort(key=lambda x: int(x.name))
        return numbered_folders
    
    def _get_images_from_folder(self, folder_path: Path):
        """Get all images from a specific folder"""
        image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
        images = []
        
        for file_path in folder_path.iterdir():
            if file_path.is_file() and file_path.suffix.lower() in image_extensions:
                images.append(file_path)
        
        return images
    
    def get_random_image(self, image_type: str, room_id: str) -> tuple:
        """Get a random image from the current folder for this image type and room - ALWAYS RETURN FILE PATH"""
        if image_type not in self.image_folders:
            logger.warning(f"❌ [{self.account_username}] Invalid image type requested: {image_type}")
            return None, None
        
        # Initialize folder usage tracking for this room
        if room_id not in self.folder_usage:
            self.folder_usage[room_id] = {}
        if image_type not in self.folder_usage[room_id]:
            self.folder_usage[room_id][image_type] = []
        
        # Get all numbered folders for this image type
        numbered_folders = self._get_numbered_folders(image_type)
        if not numbered_folders:
            logger.warning(f"❌ [{self.account_username}] No numbered folders found for {image_type}")
            return None, None
        
        # Find the first folder that hasn't been fully consumed
        current_folder = None
        current_folder_number = None
        
        for folder in numbered_folders:
            folder_num = int(folder.name)
            if folder_num not in self.folder_usage[room_id][image_type]:
                current_folder = folder
                current_folder_number = folder_num
                break
        
        if not current_folder:
            logger.info(f"📭 [{self.account_username}] All folders consumed for {image_type} in room {room_id[:8]}")
            return None, None
        
        # Get all images from current folder
        folder_images = self._get_images_from_folder(current_folder)
        if not folder_images:
            logger.warning(f"❌ [{self.account_username}] No images found in folder {current_folder_number} for {image_type}")
            # Mark folder as used and try next one
            self.folder_usage[room_id][image_type].append(current_folder_number)
            return self.get_random_image(image_type, room_id)
        
        # Initialize sent images tracking for this room (check database)
        if room_id not in self.sent_images:
            # Load previously sent images from database for this room
            self._load_sent_images_for_room(room_id)
        
        # Filter to unsent images from current folder (check both in-memory AND database)
        unsent_folder_images = []
        for img_path in folder_images:
            img_filename = img_path.name
            
            # Check if already sent (using database check)
            if not self._is_image_sent_to_room(room_id, img_filename):
                unsent_folder_images.append(img_path)
        
        if not unsent_folder_images:
            # All images in current folder have been sent - mark folder as used
            logger.info(f"✅ [{self.account_username}] Completed folder {current_folder_number} for {image_type} in room {room_id[:8]}")
            self.folder_usage[room_id][image_type].append(current_folder_number)
            
            # Try next folder
            return self.get_random_image(image_type, room_id)
        
        # Select random image from unsent images in current folder
        selected_image = random.choice(unsent_folder_images)
        image_filename = selected_image.name
        
        # REMOVED: Cache checking - always return file path for fresh upload
        # cached_url = self.url_cache.get_cached_url(image_type, image_filename)
        # 
        # if cached_url:
        #     logger.info(f"🔄 [{self.account_username}] Using cached URL for {image_type} image from folder {current_folder_number}: {image_filename}")
        #     # Mark as sent in database
        #     self._mark_image_sent_to_room(room_id, image_filename, image_type, current_folder_number)
        #     return cached_url, None
        
        logger.info(f"📸 [{self.account_username}] Returning FRESH image for upload: {image_type} image from folder {current_folder_number}: {image_filename}")
        # Mark as sent in database
        self._mark_image_sent_to_room(room_id, image_filename, image_type, current_folder_number)
        return None, str(selected_image.resolve())
        
    def _load_sent_images_for_room(self, room_id: str):
        """Load previously sent images for a room from database into memory"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT image_filename FROM sent_images 
                    WHERE account_username = %s AND room_id = %s
                """, (self.account_username, room_id))
                
                results = cursor.fetchall()
                sent_filenames = [row['image_filename'] for row in results]
                
                self.sent_images[room_id] = sent_filenames
                logger.debug(f"📁 [{self.account_username}] Loaded {len(sent_filenames)} previously sent images for room {room_id[:8]} from database")
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error loading sent images for room {room_id[:8]}: {e}")
            self.sent_images[room_id] = []
    
    def get_random_random_image(self, room_id: str) -> tuple:
        """Get a random image from random folder (NO CACHING, ALWAYS FRESH UPLOAD)"""
        try:
            folder_path = self.images_base / 'random'
            if not folder_path.exists():
                logger.warning(f"❌ [{self.account_username}] Random image folder does not exist")
                return None, None
            
            image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp'}
            available_images = []
            
            for file_path in folder_path.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in image_extensions:
                    available_images.append(file_path)
            
            if not available_images:
                logger.warning(f"❌ [{self.account_username}] No images found in random folder")
                return None, None
            
            # Load previously sent images for this room
            if room_id not in self.sent_images:
                self._load_sent_images_for_room(room_id)
            
            # Check database for sent images
            unsent_images = []
            for img_path in available_images:
                img_filename = img_path.name
                if not self._is_image_sent_to_room(room_id, img_filename):
                    unsent_images.append(img_path)
            
            if not unsent_images:
                logger.info(f"📸 [{self.account_username}] All random images have already been sent to room {room_id[:8]}. No more images available.")
                return None, None
            
            selected_image = random.choice(unsent_images)
            image_filename = selected_image.name
            
            # REMOVED: Cache checking - always return file path
            # cached_url = self.url_cache.get_cached_url('random', image_filename)
            # 
            # if cached_url:
            #     logger.info(f"🔄 [{self.account_username}] Using cached URL for random image: {image_filename}")
            #     # Mark as sent in database
            #     self._mark_image_sent_to_room(room_id, image_filename, 'random')
            #     return cached_url, None
            
            logger.info(f"📸 [{self.account_username}] Returning FRESH random image for upload: {image_filename}")
            # Mark as sent in database
            self._mark_image_sent_to_room(room_id, image_filename, 'random')
            return None, str(selected_image.resolve())
        
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting random image: {e}")
            return None, None
            
    def recover_sent_images_from_existing_conversations(self):
        """Recover sent images from existing rooms in the database"""
        try:
            # Get all rooms we have conversations with
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT DISTINCT room_id FROM conversation_states 
                    WHERE account_username = %s
                """, (self.account_username,))
                
                rooms = cursor.fetchall()
                
                for row in rooms:
                    room_id = row['room_id']
                    # This will load sent images for each room from database
                    if room_id not in self.sent_images:
                        self._load_sent_images_for_room(room_id)
                
                logger.info(f"✅ [{self.account_username}] Recovered sent images for {len(rooms)} existing conversations from database")
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error recovering sent images from existing conversations: {e}")
    
    
    def has_images_for_type(self, image_type: str) -> bool:
        """Check if we have images available for a specific type"""
        if image_type not in self.image_folders:
            return False
        
        type_folder = self.images_base / image_type
        if not type_folder.exists():
            return False
        
        # Check ALL numbered folders
        numbered_folders = self._get_numbered_folders(image_type)
        if not numbered_folders:
            return False
        
        # Check if ANY folder has images
        for folder in numbered_folders:
            folder_images = self._get_images_from_folder(folder)
            if folder_images:
                return True
        
        return False
        

    
    def has_unsent_images_for_type(self, image_type: str, room_id: str) -> bool:
        """Check if we have unsent images available for a specific type and room"""
        if image_type not in self.image_folders:
            return False
        
        type_folder = self.images_base / image_type
        if not type_folder.exists():
            return False
        
        # Get all numbered folders
        numbered_folders = self._get_numbered_folders(image_type)
        if not numbered_folders:
            return False
        
        # Initialize tracking for this room/image_type if needed
        if room_id not in self.folder_usage:
            self.folder_usage[room_id] = {}
        if image_type not in self.folder_usage[room_id]:
            self.folder_usage[room_id][image_type] = []
        
        # Check each folder in order
        for folder in numbered_folders:
            folder_num = int(folder.name)
            
            # Skip folders that have been marked as used
            if folder_num in self.folder_usage[room_id][image_type]:
                continue
            
            # Get images from this folder
            folder_images = self._get_images_from_folder(folder)
            if not folder_images:
                continue
            
            # Check if any images in this folder haven't been sent
            if room_id not in self.sent_images:
                return True  # No images sent yet to this room
            
            sent_filenames = [os.path.basename(img) for img in self.sent_images[room_id]]
            unsent_images = [img for img in folder_images if img.name not in sent_filenames]
            
            if unsent_images:
                return True
        
        # No unsent images found in any folder
        return False


class VeniceAI:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.venice.ai/api/v1"
        self.direct_session = requests.Session()
        # REMOVE: self.outgoing_ai = VeniceAIOutgoing(api_key)  # This class is now removed
        self.max_retries = 5  # Increased retries
        self.retry_delay_base = 2  # Base delay in seconds
        logger.info(f"[VENICE AI] Initialized with {self.max_retries} max retries")
        
    
    def generate_response(self, conversation_history: list, config: ChatBotConfig, current_stage: str, image_description: str = None) -> str:
        """Generate a response using Venice AI with RETRY UNTIL SUCCESS - WITH DEBUG"""
        for attempt in range(self.max_retries):
            try:
                conversation_context = self._build_conversation_context(conversation_history)
                prompt = self._build_comprehensive_prompt(conversation_context, config, current_stage, image_description)
                
                # DEBUG: Log what we're sending to Venice AI
                logger.info(f"[DEBUG VENICE AI INPUT] Attempt {attempt + 1}")
                logger.info(f"[DEBUG CONVERSATION CONTEXT]: {conversation_context[:500]}...")
                logger.info(f"[DEBUG CURRENT STAGE]: {current_stage}")
                if image_description:
                    logger.info(f"[DEBUG IMAGE DESCRIPTION]: {image_description[:200]}...")
                logger.info(f"[DEBUG PROMPT LENGTH]: {len(prompt)} chars")
                logger.info(f"[DEBUG PROMPT PREVIEW]: {prompt[:300]}...")
                
                payload = {
                    "model": "venice-uncensored",
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 150,
                    "temperature": 0.7
                }
                
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                }
                
                logger.info(f"[VENICE AI] Generating response (attempt {attempt + 1}/{self.max_retries})...")
                
                # DEBUG: Log the full API call details
                logger.debug(f"[DEBUG API CALL] URL: {self.base_url}/chat/completions")
                logger.debug(f"[DEBUG API CALL] Headers: {headers}")
                logger.debug(f"[DEBUG API CALL] Payload keys: {list(payload.keys())}")
                
                response = self._make_request_with_retry(
                    f"{self.base_url}/chat/completions",
                    method='POST',
                    json=payload,
                    headers=headers,
                    timeout=30,
                    use_direct_ip=True
                )
                
                if not response:
                    raise Exception("No response from Venice AI API")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Check if response has expected structure
                    if 'choices' not in data or len(data['choices']) == 0:
                        logger.error(f"[VENICE AI] Invalid response structure: {data}")
                        raise Exception(f"Invalid API response structure: {data}")
                    
                    raw_response = data['choices'][0]['message']['content'].strip()
                    
                    # DEBUG: Log the raw response from Venice AI
                    logger.info(f"[DEBUG VENICE AI RAW RESPONSE]: {raw_response[:500]}...")
                    
                    # Clean response
                    generated_text = self._clean_response(raw_response)
                    logger.info(f"[DEBUG AFTER CLEANING]: {generated_text[:500]}...")
                    
                    # Apply text breaking
                    generated_text = self._break_into_texts(generated_text)
                    logger.info(f"[DEBUG AFTER TEXT BREAKING]: {generated_text[:500]}...")
                    
                    logger.info(f"[VENICE AI] Successfully generated response on attempt {attempt + 1}")
                    logger.info(f"[DEBUG FINAL RESPONSE]: {generated_text}")
                    
                    return generated_text
                
                else:
                    # Log the error but continue retrying
                    error_msg = f"API Error {response.status_code}: {response.text[:200] if response.text else 'No error text'}"
                    logger.warning(f"[VENICE AI] {error_msg} - Will retry...")
                    
                    # Don't raise exception yet, will retry
                    if attempt < self.max_retries - 1:
                        wait_time = self.retry_delay_base * (2 ** attempt)  # Exponential backoff
                        logger.info(f"[VENICE AI] Waiting {wait_time}s before retry {attempt + 2}/{self.max_retries}...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Failed after {self.max_retries} attempts: {error_msg}")
                    
            except requests.exceptions.Timeout:
                logger.warning(f"[VENICE AI] Timeout on attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay_base * (2 ** attempt)
                    logger.info(f"[VENICE AI] Waiting {wait_time}s after timeout...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"Timeout after {self.max_retries} attempts")
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"[VENICE AI] Connection error on attempt {attempt + 1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay_base * (2 ** attempt)
                    logger.info(f"[VENICE AI] Waiting {wait_time}s after connection error...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"Connection failed after {self.max_retries} attempts: {e}")
                    
            except Exception as e:
                logger.error(f"[VENICE AI] Error on attempt {attempt + 1}/{self.max_retries}: {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay_base * (2 ** attempt)
                    logger.info(f"[VENICE AI] Waiting {wait_time}s after error...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"Failed after {self.max_retries} attempts: {e}")
        
        # If we get here (shouldn't happen due to exceptions above), use emergency fallback
        logger.critical("[VENICE AI] CRITICAL: All retries exhausted, using emergency fallback")
        return self._emergency_fallback(conversation_history, current_stage, image_description)
    
    def _emergency_fallback(self, conversation_history: list, current_stage: str, image_description: str = None) -> str:
        """Emergency fallback when Venice AI completely fails after all retries"""
        try:
            # Try to create a simple response based on context
            if image_description and "I see you sent an image" not in image_description:
                return random.choice([
                    "Love that pic! 😍",
                    "Looking hot in that pic! 🔥",
                    "That's an amazing photo! 😘"
                ])
            
            # Get last user message
            last_user_msg = ""
            for msg in reversed(conversation_history):
                if msg.get("sender") == "user":
                    last_user_msg = msg.get("text", "")
                    break
            
            if not last_user_msg:
                return random.choice([
                    "Hey there! 😊",
                    "Hi! How's it going?",
                    "What's up! 👋"
                ])
            
            # Simple stage-appropriate responses
            if current_stage == "pitch":
                return random.choice([
                    "So about my OnlyFans... it's totally worth it! 😈",
                    "My premium content is way hotter than what I post here! 🔥",
                    "You should definitely check out my OnlyFans for the good stuff! 😘"
                ])
            elif current_stage == "flirty":
                return random.choice([
                    "You're getting me all excited! 😉",
                    "I'm getting so turned on talking to you! 🔥",
                    "You know how to make a girl blush! 😊"
                ])
            else:  # intro
                return random.choice([
                    "Thanks for messaging me! 😊",
                    "Glad you reached out! What's up?",
                    "Hey! Nice to meet you! 👋"
                ])
                
        except Exception as e:
            logger.error(f"[VENICE AI] Even emergency fallback failed: {e}")
            # Ultimate fallback
            return "Hey! 😊"
    
    def _clean_response(self, response: str) -> str:
        """Clean up the response"""
        import re
        # Remove asterisks and extra whitespace
        response = re.sub(r'\*.*?\*', '', response).strip()
        response = re.sub(r'\s+', ' ', response).strip()
        
        # Remove any quotes around the response
        if response.startswith('"') and response.endswith('"'):
            response = response[1:-1].strip()
        elif response.startswith("'") and response.endswith("'"):
            response = response[1:-1].strip()
        
        # Remove AI response prefixes
        prefixes = ['Response:', 'Message:', 'Generated:', 'Answer:', 'Here:']
        for prefix in prefixes:
            if response.lower().startswith(prefix.lower()):
                response = response[len(prefix):].strip()
        
        return response
    
    def _break_into_texts(self, response: str) -> str:
        """Break a long response into shorter, text-like messages"""
        sentences = response.split('. ')
        
        if len(sentences) <= 2:
            return response
        
        if len(sentences) == 3:
            return f"{sentences[0]}. {sentences[1]}.\n\n{sentences[2]}"
        elif len(sentences) >= 4:
            first_part = '. '.join(sentences[:2]) + '.'
            second_part = '. '.join(sentences[2:]) + '.'
            return f"{first_part}\n\n{second_part}"
        
        return response
    
    def _build_conversation_context(self, conversation_history: list) -> str:
        """Build the actual conversation context from history"""
        if not conversation_history:
            return "This is the start of the conversation."
        
        context = "Previous conversation:\n"
        for msg in conversation_history[-6:]:
            speaker = "Them" if msg["sender"] == "user" else "You"
            context += f"{speaker}: {msg['text']}\n"
        
        return context
    
    def _build_comprehensive_prompt(self, conversation_context: str, config: ChatBotConfig, current_stage: str, image_description: str = None) -> str:
        """Build the comprehensive prompt"""
        phase_instructions = {
            "intro": config.get('phase1'),
            "flirty": config.get('phase2'), 
            "pitch": config.get('phase3')
        }
        
        prompt = f"""ROLE: You are the persona described below. Stay in character at all times.

PERSONA DETAILES:
{config.get('main')}

BACKGROUND:
{config.get('background')}

CHATTING STYLE:
{config.get('chatting_style')}

ONLYFANS INFO (use when relevant):
{config.get('onlyfans')}

OBJECTION HANDLING (use when user questions your authenticity):
{config.get('objections')}

CURRENT CONVERSATION PHASE: {current_stage.upper()}
{phase_instructions.get(current_stage, config.get('phase1'))}"""

        if image_description and image_description != "I see you sent an image, but I'm having trouble analyzing it right now.":
            prompt += f"""

IMAGE CONTEXT: The user just sent you an image. Here's what's in the image: {image_description}
Use this information to respond appropriately to their image. Comment on what you see in the image if relevant."""

        prompt += f"""

CONVERSATION HISTORY:
{conversation_context}

RESPOND to their last message as your persona:"""
        
        return prompt

    def _make_request_with_retry(self, url, method='GET', max_retries=3, retry_delay=20, use_direct_ip=False, **kwargs):
        """Make request with automatic retry on failure - FOR INTERNAL USE"""
        for attempt in range(max_retries):
            try:
                if use_direct_ip:
                    session_to_use = self.direct_session
                else:
                    session_to_use = requests.Session()
                
                if method.upper() == 'GET':
                    response = session_to_use.get(url, **kwargs)
                elif method.upper() == 'POST':
                    response = session_to_use.post(url, **kwargs)
                elif method.upper() == 'PUT':
                    response = session_to_use.put(url, **kwargs)
                else:
                    response = session_to_use.get(url, **kwargs)
                
                return response
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"🔄 Request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"❌ Request failed after {max_retries} attempts: {e}")
                    return None
        return None
        
    def generate_response_with_context(self, conversation_history: list, config: ChatBotConfig, current_stage: str, image_description: str = None, extra_context: str = None) -> str:
        """Generate a response using Venice AI with optional extra context"""
        for attempt in range(self.max_retries):
            try:
                conversation_context = self._build_conversation_context(conversation_history)
                prompt = self._build_comprehensive_prompt_with_extra(conversation_context, config, current_stage, image_description, extra_context)
                
                payload = {
                    "model": "venice-uncensored",
                    "messages": [
                        {
                            "role": "user",
                            "content": prompt
                        }
                    ],
                    "max_tokens": 150,
                    "temperature": 0.7
                }
                
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                }
                
                logger.info(f"[VENICE AI] Generating response with extra context (attempt {attempt + 1}/{self.max_retries})...")
                
                response = self._make_request_with_retry(
                    f"{self.base_url}/chat/completions",
                    method='POST',
                    json=payload,
                    headers=headers,
                    timeout=30,
                    use_direct_ip=True
                )
                
                if not response:
                    raise Exception("No response from Venice AI API")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'choices' not in data or len(data['choices']) == 0:
                        logger.error(f"[VENICE AI] Invalid response structure: {data}")
                        raise Exception(f"Invalid API response structure: {data}")
                    
                    generated_text = data['choices'][0]['message']['content'].strip()
                    
                    # Clean response
                    generated_text = self._clean_response(generated_text)
                    generated_text = self._break_into_texts(generated_text)
                    
                    logger.info(f"[VENICE AI] Successfully generated response with extra context on attempt {attempt + 1}")
                    return generated_text
                
                else:
                    # Log the error but continue retrying
                    error_msg = f"API Error {response.status_code}: {response.text[:200] if response.text else 'No error text'}"
                    logger.warning(f"[VENICE AI] {error_msg} - Will retry...")
                    
                    if attempt < self.max_retries - 1:
                        wait_time = self.retry_delay_base * (2 ** attempt)
                        logger.info(f"[VENICE AI] Waiting {wait_time}s before retry {attempt + 2}/{self.max_retries}...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise Exception(f"Failed after {self.max_retries} attempts: {error_msg}")
                    
            except Exception as e:
                logger.error(f"[VENICE AI] Error generating response with extra context (attempt {attempt + 1}/{self.max_retries}): {e}")
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay_base * (2 ** attempt)
                    logger.info(f"[VENICE AI] Waiting {wait_time}s after error...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception(f"Failed after {self.max_retries} attempts: {e}")
        
        # Fallback to regular generation if retries fail
        logger.critical("[VENICE AI] CRITICAL: All retries exhausted for response with extra context")
        return self.generate_response(conversation_history, config, current_stage, image_description)
    
    def _build_comprehensive_prompt_with_extra(self, conversation_context: str, config: ChatBotConfig, current_stage: str, image_description: str = None, extra_context: str = None) -> str:
        """Build the comprehensive prompt with extra context"""
        phase_instructions = {
            "intro": config.get('phase1'),
            "flirty": config.get('phase2'), 
            "pitch": config.get('phase3')
        }
        
        prompt = f"""ROLE: You are the persona described below. Stay in character at all times.

PERSONA DETAILES:
{config.get('main')}

BACKGROUND:
{config.get('background')}

CHATTING STYLE:
{config.get('chatting_style')}

ONLYFANS INFO (use when relevant):
{config.get('onlyfans')}

OBJECTION HANDLING (use when user questions your authenticity):
{config.get('objections')}

CURRENT CONVERSATION PHASE: {current_stage.upper()}
{phase_instructions.get(current_stage, config.get('phase1'))}"""

        if extra_context:
            prompt += f"\n\n{extra_context}"

        if image_description and image_description != "I see you sent an image, but I'm having trouble analyzing it right now.":
            prompt += f"""

IMAGE CONTEXT: The user just sent you an image. Here's what's in the image: {image_description}
Use this information to respond appropriately to their image. Comment on what you see in the image if relevant."""

        prompt += f"""

CONVERSATION HISTORY:
{conversation_context}

RESPOND to their last message as your persona:"""
        
        return prompt 

# ========== CHAT INTERFACE HANDLER ==========
class ChatInterfaceHandler:
    """Handles direct chat interface interaction via Selenium"""
    
    def __init__(self, account_username: str, adspower_profile_id: str = None, matrix_chat_parent=None):
        self.account_username = account_username
        self.adspower_profile_id = adspower_profile_id
        self.matrix_chat_parent = matrix_chat_parent  # NEW: Store reference to parent matrix_chat
        self.browser = None
        self.current_room = None
        self.last_navigation_time = 0
        self.navigation_cooldown = 2
        
        # REMOVE token validation dependency
        self.last_token_check = 0
        self.token_check_interval = 300  # Only check every 5 minutes
        
        logger.info(f"💬 [{self.account_username}] Chat Interface Handler initialized")
    
    def initialize_browser(self) -> bool:
        """Initialize AdsPower browser for chat interaction - MODIFIED TO HANDLE ALL VALID CHAT URLS"""
        if not self.adspower_profile_id:
            logger.error(f"❌ [{self.account_username}] No AdsPower profile ID configured for chat interface")
            return False
        
        try:
            self.browser = AdsPowerChatBrowser(base_url="http://local.adspower.net:50325")
            self.browser.set_profile(self.adspower_profile_id)
            
            if not self.browser.start_browser():
                logger.error(f"❌ [{self.account_username}] Failed to start browser for chat interface")
                return False
            
            # MODIFIED: Check current URL first
            current_url = self.browser.driver.current_url if self.browser.driver else ""
            
            # Check if we're already on a valid chat interface URL
            valid_patterns = [
                "chat.reddit.com",
                "reddit.com/chat/room/",
                "reddit.com/chat/",  # Add this for old chat interface
                "www.reddit.com/chat/"  # Add this for www version
            ]
            
            is_valid_chat_url = False
            if current_url:
                is_valid_chat_url = any(pattern in current_url for pattern in valid_patterns)
            
            if not is_valid_chat_url:
                # Only navigate if we're not already on a chat URL
                if not self.browser.navigate_to_chat():
                    logger.error(f"❌ [{self.account_username}] Failed to navigate to chat interface")
                    return False
            
            logger.info(f"✅ [{self.account_username}] Chat interface browser initialized")
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error initializing chat interface: {e}")
            return False
   
    def navigate_to_room(self, room_id: str) -> bool:
        """Navigate to a specific chat room"""
        try:
            current_time = time.time()
            if current_time - self.last_navigation_time < self.navigation_cooldown:
                wait_time = self.navigation_cooldown - (current_time - self.last_navigation_time)
                logger.debug(f"⏳ [{self.account_username}] Cooling down navigation for {wait_time:.1f}s")
                time.sleep(wait_time)
            
            if not self.browser or not self.browser.driver:
                if not self.initialize_browser():
                    return False
            
            room_url = f"https://chat.reddit.com/room/{room_id}"
            logger.info(f"📍 [{self.account_username}] Navigating to room: {room_url}")
            
            self.browser.driver.get(room_url)
            
            # Wait for page to load
            WebDriverWait(self.browser.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for chat interface to load
            time.sleep(3)
            
            # Check if we're on the correct page
            current_url = self.browser.driver.current_url
            if room_id in current_url:
                self.current_room = room_id
                self.last_navigation_time = time.time()
                logger.info(f"✅ [{self.account_username}] Successfully navigated to room {room_id[:8]}")
                return True
            else:
                logger.error(f"❌ [{self.account_username}] Failed to navigate to room {room_id[:8]}. Current URL: {current_url}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error navigating to room {room_id[:8]}: {e}")
            return False

    def find_message_input(self):
        """Find the message input element - SIMPLIFIED VERSION"""
        if not self.browser or not self.browser.driver:
            return None
        
        driver = self.browser.driver
        
        # FIRST: Try the JavaScript path from Developer Tools
        try:
            js_path = '''
            return document.querySelector("body > faceplate-app > rs-app").shadowRoot
            .querySelector("div.rs-app-container > div > rs-page-overlay-manager > rs-room").shadowRoot
            .querySelector("main > rs-message-composer").shadowRoot
            .querySelector("div > form > div > div.h-fit.py-\\\\[var\\\\(--rem10\\\\)\\\\] > textarea")
            '''
            
            logger.info(f"🔍 [{self.account_username}] Trying direct JavaScript path...")
            element = driver.execute_script(js_path)
            
            if element:
                logger.info(f"✅ [{self.account_username}] Found message input via JavaScript")
                return element
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] JavaScript path failed: {e}")
        
        # SECOND: Try to find any textarea
        try:
            # Use JavaScript to find any textarea in the document
            find_any_js = """
            // Simple function to find any textarea
            function findTextarea() {
                // First try common selectors
                let selectors = [
                    'textarea[name="message"]',
                    'textarea[placeholder*="Message"]',
                    'textarea[placeholder*="message"]',
                    'textarea'
                ];
                
                for (let selector of selectors) {
                    let el = document.querySelector(selector);
                    if (el) return el;
                }
                
                return null;
            }
            
            return findTextarea();
            """
            
            element = driver.execute_script(find_any_js)
            if element:
                logger.info(f"✅ [{self.account_username}] Found textarea via simple search")
                return element
                
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Simple search failed: {e}")
        
        logger.warning(f"⚠️ [{self.account_username}] Could not find message input element")
        return None

    def send_message(self, room_id: str, message: str) -> bool:
        """Send message via chat interface navigation - NO TOKEN/COOKIE CHECKS"""
        try:
            logger.info(f"💬 [{self.account_username}] Sending message to room {room_id[:8]} via chat interface")
            
            # REMOVED: No token/connection validation needed for browser interface
            
            # Navigate to the room
            if not self.chat_interface.navigate_to_room(room_id):
                logger.error(f"❌ [{self.account_username}] Failed to navigate to room {room_id[:8]}")
                return False
            
            # Wait for page to load (it automatically focuses the textarea)
            time.sleep(2)
            
            # Send the message using simple strategy
            success = self.chat_interface.send_message_to_current_room(message)
            
            if success:
                # Update last reply timestamp in database
                timestamp = int(time.time() * 1000)
                self.persistent_state.update_last_reply_timestamp(room_id, timestamp)
                logger.debug(f"📝 [{self.account_username}] Updated last reply timestamp for room {room_id[:8]} to {timestamp}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error sending message via chat interface: {e}")
            return False

    def find_message_input_enhanced(self):
        """Enhanced method to find message input with multiple strategies"""
        if not self.browser or not self.browser.driver:
            return None
        
        driver = self.browser.driver
        
        # Strategy 1: Look for contenteditable elements (most common in modern Reddit chat)
        try:
            content_editables = driver.find_elements(By.CSS_SELECTOR, "[contenteditable='true']")
            for element in content_editables:
                if element.is_displayed() and element.is_enabled():
                    logger.info(f"✅ [{self.account_username}] Found contenteditable message input")
                    return element
        except:
            pass
        
        # Strategy 2: Look for textareas
        try:
            textareas = driver.find_elements(By.TAG_NAME, "textarea")
            for textarea in textareas:
                if textarea.is_displayed() and textarea.is_enabled():
                    placeholder = textarea.get_attribute("placeholder") or ""
                    if "message" in placeholder.lower() or "type" in placeholder.lower():
                        logger.info(f"✅ [{self.account_username}] Found textarea message input")
                        return textarea
        except:
            pass
        
        # Strategy 3: Look for input with message-related attributes
        try:
            inputs = driver.find_elements(By.TAG_NAME, "input")
            for input_elem in inputs:
                if input_elem.is_displayed() and input_elem.is_enabled():
                    placeholder = input_elem.get_attribute("placeholder") or ""
                    name = input_elem.get_attribute("name") or ""
                    if "message" in placeholder.lower() or "message" in name.lower():
                        logger.info(f"✅ [{self.account_username}] Found input message input")
                        return input_elem
        except:
            pass
        
        # Strategy 4: JavaScript search
        try:
            js_finder = """
            function findMessageInput() {
                // Try contenteditable first
                let contentEditables = document.querySelectorAll("[contenteditable='true']");
                for (let el of contentEditables) {
                    if (el.offsetWidth > 0 && el.offsetHeight > 0) {
                        return el;
                    }
                }
                
                // Try textareas with message placeholders
                let textareas = document.querySelectorAll("textarea");
                for (let ta of textareas) {
                    if (ta.offsetWidth > 0 && ta.offsetHeight > 0) {
                        let placeholder = ta.placeholder || "";
                        if (placeholder.toLowerCase().includes("message") || 
                            placeholder.toLowerCase().includes("type")) {
                            return ta;
                        }
                    }
                }
                
                // Try inputs
                let inputs = document.querySelectorAll("input[type='text'], input[type='textarea']");
                for (let inp of inputs) {
                    if (inp.offsetWidth > 0 && inp.offsetHeight > 0) {
                        let placeholder = inp.placeholder || "";
                        let name = inp.name || "";
                        if (placeholder.toLowerCase().includes("message") || 
                            name.toLowerCase().includes("message")) {
                            return inp;
                        }
                    }
                }
                
                return null;
            }
            return findMessageInput();
            """
            
            element = driver.execute_script(js_finder)
            if element:
                logger.info(f"✅ [{self.account_username}] Found message input via JavaScript")
                return element
        except:
            pass
        
        logger.warning(f"⚠️ [{self.account_username}] Could not find message input element")
        return None


    def send_image_to_current_room(self, image_path_or_url: str, matrix_token: str = None) -> bool:
        """
        Send an image to the current room.
        Can handle either file path or cached MXC URL.
        """
        try:
            if not self.current_room:
                logger.error(f"❌ [{self.account_username}] No current room set for image sending")
                return False
            
            if not self.browser or not self.browser.driver:
                logger.error(f"❌ [{self.account_username}] Browser not initialized for image sending")
                return False
            
            logger.info(f"📸 [{self.account_username}] Preparing to send image: {os.path.basename(image_path_or_url) if not image_path_or_url.startswith('mxc://') else 'cached URL'} to current room")
            
            # Check if it's a cached URL or a file path
            if image_path_or_url.startswith('mxc://'):
                # It's a cached MXC URL - use API upload + manual send
                return self.send_image_api_then_manual(image_path_or_url, matrix_token)
            else:
                # It's a file path - upload via API then send manually
                return self.send_image_api_then_manual(image_path_or_url, matrix_token)
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error in send_image_to_current_room: {e}")
            return False

    def upload_image_to_matrix(self, image_path: str, matrix_token: str = None):
        """Upload image to Matrix media repository and get MXC URL"""
        try:
            if not matrix_token:
                logger.error(f"❌ [{self.account_username}] No Matrix token provided for upload")
                return None, None
            
            # 1. Get image dimensions
            try:
                from PIL import Image
                with Image.open(image_path) as img:
                    width, height = img.size
            except ImportError:
                width, height = 800, 600
            except Exception as e:
                logger.warning(f"⚠️ [{self.account_username}] Could not get image dimensions: {e}")
                width, height = 800, 600
            
            # 2. Get file info
            file_size = os.path.getsize(image_path)
            mimetype = mimetypes.guess_type(image_path)[0] or 'image/jpeg'
            filename = os.path.basename(image_path)
            
            # 3. Construct upload URL
            upload_url = f"https://matrix.redditspace.com/_matrix/media/v3/upload?filename={filename}"
            
            # 4. Set headers
            headers = {
                'Authorization': f'Bearer {matrix_token}',
                'Content-Type': mimetype,
                'Origin': 'https://chat.reddit.com',
                'Referer': 'https://chat.reddit.com/',
            }
            
            # 5. Read image file
            try:
                with open(image_path, 'rb') as f:
                    image_data = f.read()
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Error reading image file: {e}")
                return None, None
            
            # 6. Make the upload request
            for attempt in range(3):
                try:
                    session = requests.Session()
                    response = session.post(
                        upload_url,
                        data=image_data,
                        headers=headers,
                        timeout=60,
                        verify=False
                    )
                    
                    # 7. Process response
                    if response.status_code == 200:
                        upload_data = response.json()
                        mxc_url = upload_data.get('content_uri')
                        
                        if mxc_url:
                            # 8. Return MXC URL + image metadata
                            image_info = {
                                "w": width,
                                "h": height,
                                "mimetype": mimetype,
                                "size": file_size
                            }
                            logger.info(f"✅ [{self.account_username}] Image uploaded successfully! MXC URL: {mxc_url[:50]}...")
                            return mxc_url, image_info
                    else:
                        logger.error(f"❌ [{self.account_username}] Image upload failed: {response.status_code}")
                        if attempt < 2:
                            time.sleep(20)
                        
                except Exception as e:
                    logger.error(f"❌ [{self.account_username}] Image upload error on attempt {attempt + 1}: {e}")
                    if attempt < 2:
                        time.sleep(20)
                    continue
            
            logger.error(f"❌ [{self.account_username}] Image upload failed after 3 attempts")
            return None, None
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error in upload_image_to_matrix: {e}")
            return None, None
            

    def send_image_with_mxc_url(self, mxc_url: str, image_info: dict = None):
        """Send already-uploaded image via API (not browser)"""
        try:
            if not self.current_room:
                logger.error(f"❌ [{self.account_username}] No current room set for sending image")
                return False
            
            if not self.matrix_chat_parent:
                logger.error(f"❌ [{self.account_username}] No parent matrix chat reference for API sending")
                return False
            
            # Get matrix token from parent
            matrix_token = self.matrix_chat_parent.matrix_token
            if not matrix_token:
                logger.error(f"❌ [{self.account_username}] No Matrix token available from parent")
                return False
            
            logger.info(f"📤 [{self.account_username}] Sending uploaded image via API to room {self.current_room[:8]}")
            
            # Use the API to send the image
            success = self.send_image_message(self.current_room, mxc_url, image_info, matrix_token)
            
            if success:
                logger.info(f"✅ [{self.account_username}] Image sent successfully via API")
            else:
                logger.error(f"❌ [{self.account_username}] Failed to send image via API")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error in send_image_with_mxc_url: {e}")
            return False
            
    def send_image_api_then_manual(self, image_path: str, matrix_token: str = None) -> bool:
        """
        Upload image via API, then send via API.
        """
        try:
            logger.info(f"🚀 [{self.account_username}] Starting API upload + API send: {os.path.basename(image_path)}")
            
            # Step 1: Upload image via API with provided token
            mxc_url, image_info = self.upload_image_to_matrix(image_path, matrix_token)
            if not mxc_url:
                logger.error(f"❌ [{self.account_username}] Failed to upload image via API")
                return False
            
            logger.info(f"✅ [{self.account_username}] Image uploaded to Matrix server: {mxc_url[:50]}...")
            
            # Step 2: Wait a moment
            time.sleep(1)
            
            # Step 3: Send the uploaded image via API
            success = self.send_image_with_mxc_url(mxc_url, image_info)
            
            if success:
                logger.info(f"✅ [{self.account_username}] Image uploaded and sent via API successfully!")
            else:
                logger.warning(f"⚠️ [{self.account_username}] Image uploaded but API send failed")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error in API upload + API send: {e}")
            return False
            
    def send_message_to_current_room(self, message: str) -> bool:
        """Send message to the current room - BROWSER ONLY, NO TOKEN CHECKS"""
        try:
            if not self.current_room:
                logger.error(f"❌ [{self.account_username}] No current room set")
                return False
            
            if not self.browser or not self.browser.driver:
                logger.error(f"❌ [{self.account_username}] Browser not initialized")
                return False
            
            logger.info(f"⌨️ [{self.account_username}] Typing message in browser: {message[:50]}...")
            
            driver = self.browser.driver
            
            # Wait for page to be ready
            time.sleep(2)
            
            # Type message directly into active textarea (Reddit chat automatically focuses it)
            actions = ActionChains(driver)
            
            # Type character by character (human-like)
            for char in message:
                actions.send_keys(char)
                time.sleep(random.uniform(0.03, 0.07))  # Human typing speed
            
            # Press Enter to send
            actions.send_keys(Keys.RETURN)
            actions.perform()
            
            logger.info(f"✅ [{self.account_username}] Message typed and sent via browser")
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error sending message via browser: {e}")
            return False

    def send_image_message(self, room_id: str, mxc_url: str, image_info: dict = None, matrix_token: str = None):
        """Send image message to Matrix room using cached MXC URL via API"""
        try:
            # 1. Check auth - use provided token or parent's token
            token_to_use = matrix_token
            if not token_to_use and self.matrix_chat_parent:
                token_to_use = self.matrix_chat_parent.matrix_token
            
            if not token_to_use:
                logger.error(f"❌ [{self.account_username}] No Matrix token available for sending image")
                return False
            
            # 2. Prepare image metadata (if not provided, use defaults)
            if not image_info:
                image_info = {
                    "w": 800,
                    "h": 600,
                    "mimetype": "image/jpeg",
                    "size": 100000
                }
            
            # 3. Generate unique transaction ID
            txn_id = f"m{int(time.time() * 1000)}.1"
            
            # 4. Construct the send URL
            matrix_base = "https://matrix.redditspace.com/_matrix/client/v3"
            url = f"{matrix_base}/rooms/{room_id}/send/m.room.message/{txn_id}"
            
            # 5. Create the message payload
            payload = {
                "msgtype": "m.image",  # Matrix message type for images
                "body": "😘",  # Fallback text (shown if image doesn't load)
                "url": mxc_url,  # The MXC URL from upload
                "info": image_info  # Image metadata
            }
            
            # 6. Set headers
            headers = {
                'Authorization': f'Bearer {token_to_use}',
                'Content-Type': 'application/json'
            }
            
            # 7. Make the PUT request to send the message
            logger.info(f"📤 [{self.account_username}] Sending image message via API to room {room_id[:8]}")
            response = requests.put(
                url,
                json=payload,
                headers=headers,
                timeout=30,
                verify=False
            )
            
            # 8. Check response
            if response.status_code == 200:
                data = response.json()
                event_id = data.get('event_id')
                logger.info(f"✅ [{self.account_username}] Image sent successfully via API to room {room_id[:8]}")
                return True
            else:
                logger.error(f"❌ [{self.account_username}] Failed to send image via API: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error sending image message: {e}")
            return False

    def navigate_and_update_token(self, room_id: str, matrix_chat) -> bool:
        """Navigate to room AND update token from new page"""
        try:
            logger.info(f"📍 [{self.account_username}] Navigating to room: {room_id[:8]}")
            
            if not self.browser or not self.browser.driver:
                return False
            
            room_url = f"https://chat.reddit.com/room/{room_id}"
            self.browser.driver.get(room_url)
            
            # Wait for page to load
            WebDriverWait(self.browser.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            time.sleep(3)
            
            # Update token from new page (async)
            if matrix_chat:
                threading.Thread(
                    target=matrix_chat.get_current_token_from_browser,
                    name=f"{self.account_username}_nav_token_update"
                ).start()
            
            self.current_room = room_id
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Navigation error: {e}")
            return False

    def debug_chat_interface(self):
        """Debug the chat interface structure with shadow DOM analysis"""
        try:
            logger.info(f"🔍 [{self.account_username}] DEBUG: Analyzing chat interface with shadow DOM...")
            
            driver = self.browser.driver
            
            # Get current URL
            current_url = driver.current_url
            logger.info(f"📍 [{self.account_username}] Current URL: {current_url}")
            
            # Get page source for debugging
            page_source = driver.page_source[:5000]  # First 5000 chars
            logger.info(f"📄 [{self.account_username}] Page source snippet: {page_source}")
            
            # Advanced shadow DOM analysis
            shadow_analysis_js = """
            console.log('=== SHADOW DOM ANALYSIS ===');
            
            function analyzeShadowDOM(element, depth = 0) {
                let indent = '  '.repeat(depth);
                let results = [];
                
                // Check if element has shadow root
                if (element.shadowRoot) {
                    console.log(indent + '🔍 Found shadowRoot on:', element.tagName, element.className);
                    
                    // Look for textareas in this shadow root
                    let shadowTextareas = element.shadowRoot.querySelectorAll('textarea');
                    console.log(indent + '  📝 Textareas in shadow:', shadowTextareas.length);
                    
                    shadowTextareas.forEach((ta, i) => {
                        console.log(indent + `    Textarea ${i}:`, {
                            name: ta.name,
                            placeholder: ta.placeholder,
                            'aria-label': ta.getAttribute('aria-label'),
                            value: ta.value,
                            className: ta.className
                        });
                    });
                    
                    // Look for forms
                    let shadowForms = element.shadowRoot.querySelectorAll('form');
                    console.log(indent + '  📋 Forms in shadow:', shadowForms.length);
                    
                    // Recursively analyze child elements with shadow roots
                    let shadowChildren = element.shadowRoot.querySelectorAll('*');
                    shadowChildren.forEach(child => {
                        if (child.shadowRoot) {
                            let childResults = analyzeShadowDOM(child, depth + 2);
                            results = results.concat(childResults);
                        }
                    });
                }
                
                return results;
            }
            
            // Start analysis from key elements
            let faceplateApp = document.querySelector('faceplate-app');
            let rsApp = document.querySelector('rs-app');
            
            console.log('Analyzing faceplate-app...');
            if (faceplateApp) analyzeShadowDOM(faceplateApp, 1);
            
            console.log('Analyzing rs-app...');
            if (rsApp) analyzeShadowDOM(rsApp, 1);
            
            // Try to find our specific element
            let specificElement = null;
            try {
                specificElement = document.querySelector("body > faceplate-app > rs-app").shadowRoot
                    .querySelector("div.rs-app-container > div > rs-page-overlay-manager > rs-room").shadowRoot
                    .querySelector("main > rs-message-composer").shadowRoot
                    .querySelector("div > form > div > div.h-fit.py-\\\\[var\\\\(--rem10\\\\)\\\\] > textarea");
                console.log('Specific element found via full path:', !!specificElement);
            } catch(e) {
                console.log('Full path error:', e.message);
            }
            
            return {
                hasFaceplateApp: !!faceplateApp,
                hasRsApp: !!rsApp,
                specificElementFound: !!specificElement
            };
            """
            
            try:
                result = driver.execute_script(shadow_analysis_js)
                logger.info(f"📊 [{self.account_username}] Shadow DOM analysis result: {result}")
            except Exception as e:
                logger.warning(f"⚠️ [{self.account_username}] Shadow DOM analysis failed: {e}")
            
            # Look for all textareas in the document (including shadow DOM)
            textarea_search_js = """
            // Function to find all textareas in document, including shadow DOM
            function findAllTextareas() {
                let allTextareas = [];
                
                function walkShadowDOM(node) {
                    // If node is a textarea, add it
                    if (node.tagName && node.tagName.toLowerCase() === 'textarea') {
                        allTextareas.push({
                            element: node,
                            name: node.name || '',
                            placeholder: node.placeholder || '',
                            'aria-label': node.getAttribute('aria-label') || '',
                            value: node.value || '',
                            className: node.className || '',
                            inShadow: true
                        });
                    }
                    
                    // Check shadow DOM
                    if (node.shadowRoot) {
                        let children = node.shadowRoot.querySelectorAll('*');
                        children.forEach(child => walkShadowDOM(child));
                    }
                    
                    // Check regular children
                    if (node.children) {
                        Array.from(node.children).forEach(child => walkShadowDOM(child));
                    }
                }
                
                // Start walking from document body
                walkShadowDOM(document.body);
                
                return allTextareas;
            }
            
            return findAllTextareas();
            """
            
            try:
                all_textareas = driver.execute_script(textarea_search_js)
                logger.info(f"📊 [{self.account_username}] Found {len(all_textareas)} textareas in entire document (including shadow DOM)")
                
                for i, ta in enumerate(all_textareas):
                    if ta['name'] == 'message' or 'message' in (ta['placeholder'] or '').lower():
                        logger.info(f"✅ [{self.account_username}] Found likely message textarea #{i}:")
                        logger.info(f"   Name: {ta['name']}")
                        logger.info(f"   Placeholder: {ta['placeholder']}")
                        logger.info(f"   aria-label: {ta['aria-label']}")
                        logger.info(f"   Class: {ta['className'][:50]}")
                        
            except Exception as e:
                logger.warning(f"⚠️ [{self.account_username}] Textarea search failed: {e}")
            
            return True
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error debugging chat interface: {e}")
            return False
    

    def _clear_existing_message_text(self, driver):
        """Clear any existing text in the message box BEFORE typing"""
        try:
            # Try to find any textarea
            textareas = driver.find_elements(By.TAG_NAME, "textarea")
            
            for textarea in textareas:
                try:
                    # Get current value
                    current_value = textarea.get_attribute("value") or ""
                    if current_value:
                        # Clear it
                        textarea.clear()
                        logger.debug(f"🧹 [{self.account_username}] Cleared existing text from textarea")
                        time.sleep(0.5)
                except:
                    pass
        except:
            pass

    def _send_message_single_strategy(self, driver, message: str) -> bool:
        """Use a simple strategy to send message - page is already loaded with textarea focused"""
        try:
            logger.info(f"🔍 [{self.account_username}] Using SIMPLE strategy - page already loaded")
            
            # Just wait a moment for any animations
            time.sleep(1)
            
            # Type the message directly using ActionChains (textbox is already focused)
            logger.info(f"⌨️ [{self.account_username}] Typing message: {message[:50]}...")
            
            actions = ActionChains(driver)
            
            # Type character by character
            for char in message:
                actions.send_keys(char)
                time.sleep(random.uniform(0.05, 0.1))  # Human typing speed
            
            # Press Enter ONCE to send
            actions.send_keys(Keys.RETURN)
            actions.perform()
            
            logger.info(f"✅ [{self.account_username}] Typing completed, waiting for send...")
            
            # Wait for send to complete
            time.sleep(2)
            
            # For simple strategy, we just assume it worked since we typed directly
            logger.info(f"✅ [{self.account_username}] Message sent (simple strategy)")
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Simple strategy failed: {e}")
            return False
    

    def cleanup(self):
        """Clean up browser resources"""
        try:
            if self.browser:
                self.browser.cleanup()
                logger.info(f"🧹 [{self.account_username}] Chat interface cleaned up")
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error cleaning up chat interface: {e}")

class DirectIPDownloader:
    """Handles image downloads using direct IP"""
    
    def __init__(self):
        self.direct_session = requests.Session()
        
        self.user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        ]
        
        self.setup_direct_session()
        self.download_queue = []
        self.queue_lock = threading.Lock()
        self.last_download_time = 0
        self.min_download_interval = 1
    
    def setup_direct_session(self):
        """Setup direct IP session with headers"""
        self.direct_session.headers.update({
            'Accept': 'image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'image',
            'Sec-Fetch-Mode': 'no-cors',
            'Sec-Fetch-Site': 'same-site',
        })
        
        self.direct_session.headers['User-Agent'] = random.choice(self.user_agents)
    
    def download_image(self, mxc_url: str, headers: dict = None) -> bytes:
        """Download image from Matrix server using direct IP"""
        try:
            current_time = time.time()
            time_since_last = current_time - self.last_download_time
            
            if time_since_last < self.min_download_interval:
                wait_time = self.min_download_interval - time_since_last + random.uniform(0.5, 2.0)
                logger.debug(f"⏳ Direct download: Waiting {wait_time:.2f}s before download")
                time.sleep(wait_time)
            
            if random.random() < 0.2:
                self.direct_session.headers['User-Agent'] = random.choice(self.user_agents)
            
            download_url = f"https://matrix.redditspace.com/_matrix/media/v3/download/{mxc_url.replace('mxc://', '')}"
            
            logger.info(f"⬇️ Direct IP download: {mxc_url[:50]}...")
            
            response = self.direct_session.get(
                download_url,
                headers=headers,
                timeout=30,
                verify=False
            )
            
            self.last_download_time = time.time()
            
            if response.status_code == 200:
                logger.info(f"✅ Direct IP download successful: {len(response.content)} bytes")
                return response.content
            else:
                logger.error(f"❌ Direct IP download failed: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Direct IP download error: {e}")
            return None

class AdsPowerChatBrowser:
    def __init__(self, base_url: str = "http://local.adspower.net:50325"):
        self.base_url = base_url
        self.driver = None
        self.profile_id = None
        self.session_cookies = []
        
    def cleanup(self):
        """Clean up browser session properly"""
        try:
            if self.driver:
                self.driver.quit()
                logger.info(f"🧹 Cleaned up browser session")
        except Exception as e:
            logger.debug(f"⚠️ Error cleaning up browser: {e}")
        finally:
            self.driver = None
            self.profile_id = None

    def set_profile(self, profile_id: str):
        """Set the AdsPower profile ID for this instance"""
        self.profile_id = profile_id

    def _make_request_with_retry(self, url, max_retries=3, retry_delay=20, **kwargs):
        """Make request with automatic retry on failure"""
        for attempt in range(max_retries):
            try:
                logger.info(f"🌐 AdsPower API call attempt {attempt + 1}/{max_retries}: {url}")
                response = requests.get(url, **kwargs)
                logger.info(f"📊 AdsPower API response status: {response.status_code}")
                return response
            except (requests.exceptions.RequestException, requests.exceptions.Timeout) as e:
                if attempt < max_retries - 1:
                    logger.warning(f"🔄 AdsPower request failed (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"❌ AdsPower request failed after {max_retries} attempts: {e}")
                    return None
        return None

    def start_browser(self) -> bool:
        """Connect to existing AdsPower browser"""
        if not self.profile_id:
            logger.error("❌ No profile ID set for AdsPower browser")
            return False
        
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"🔄 Attempt {attempt}/{max_retries} to connect to browser for profile {self.profile_id}")
                
                url = f"{self.base_url}/api/v1/browser/start"
                params = {
                    "user_id": self.profile_id,
                    "open_tabs": 0,
                    "ip_tab": 0,
                    "new_tab": 0,
                    "disable_images": 1,
                    "block_ads": 1,
                    "disable_video": 1,
                    "disable_audio": 1,
                    "block_3rd_party_cookies": 1,
                    "disable_notifications": 1,
                    "disable_geolocation": 1,
                    "headless": 0,
                    "remote_debug": 1,
                }
                
                logger.info(f"🚀 Connecting to existing AdsPower browser (non-headless)...")
                response = self._make_request_with_retry(url, params=params, timeout=30)
                
                if response and response.status_code == 200:
                    data = response.json()
                    
                    if data.get("code") == 0:
                        driver_data = data["data"]
                        actual_port = driver_data.get("debug_port")
                        webdriver_path = driver_data.get("webdriver")
                        
                        if actual_port and webdriver_path:
                            port_num = int(actual_port)
                            logger.info(f"📡 Connected to AdsPower browser on port: {port_num}")
                            
                            chrome_options = Options()
                            chrome_options.add_experimental_option("debuggerAddress", f"127.0.0.1:{port_num}")
                            
                            chrome_options.add_argument("--mute-audio")
                            chrome_options.add_argument("--disable-notifications")
                            chrome_options.add_argument("--no-first-run")
                            chrome_options.add_argument("--disable-background-networking")
                            chrome_options.add_argument("--disable-sync")
                            chrome_options.add_argument("--no-sandbox")
                            chrome_options.add_argument("--disable-dev-shm-usage")
                            
                            from selenium.webdriver.chrome.service import Service
                            service = Service(executable_path=webdriver_path)
                            
                            try:
                                logger.info(f"🔌 Connecting to Chrome on port {port_num}...")
                                self.driver = webdriver.Chrome(service=service, options=chrome_options)
                                logger.info("✅ Connected to Chrome!")
                                
                                time.sleep(2)
                                if len(self.driver.window_handles) > 1:
                                    main_window = self.driver.window_handles[0]
                                    for handle in self.driver.window_handles[1:]:
                                        self.driver.switch_to.window(handle)
                                        self.driver.close()
                                    self.driver.switch_to.window(main_window)
                                
                                return True
                            except Exception as e:
                                logger.error(f"❌ WebDriver connection failed: {e}")
                                
                                if attempt < max_retries:
                                    wait_time = attempt * 3
                                    logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                                    time.sleep(wait_time)
                                    continue
                                else:
                                    return False
                    else:
                        logger.error(f"❌ AdsPower API error: {data.get('msg')}")
                        if attempt < max_retries:
                            time.sleep(5)
                else:
                    logger.error(f"❌ AdsPower API HTTP error: {response.status_code if response else 'No response'}")
                    if attempt < max_retries:
                        time.sleep(5)
                        continue
                            
            except Exception as e:
                logger.error(f"❌ Browser connection error (attempt {attempt}): {e}")
                if attempt < max_retries:
                    wait_time = attempt * 3
                    logger.info(f"⏳ Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
        
        logger.error(f"❌ Failed to connect to browser after {max_retries} attempts")
        return False

    def navigate_to_chat(self) -> bool:
        """Navigate to chat.reddit.com in non-headless mode - MODIFIED TO HANDLE ALL VALID CHAT URLS"""
        try:
            logger.info("🌐 Navigating to chat.reddit.com...")
            self.driver.get("https://chat.reddit.com")
            
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            current_url = self.driver.current_url
            logger.info(f"📍 Current URL: {current_url}")
            
            # MODIFIED: Accept multiple valid chat URL patterns
            valid_patterns = [
                "chat.reddit.com",
                "reddit.com/chat/room/",
                "reddit.com/chat/",  # Add this for old chat interface
                "www.reddit.com/chat/"  # Add this for www version
            ]
            
            is_valid_chat_url = any(pattern in current_url for pattern in valid_patterns)
            
            if is_valid_chat_url:
                logger.info(f"✅ Successfully navigated to chat interface: {current_url}")
                return True
            else:
                logger.warning(f"⚠️ Not on chat interface, current URL: {current_url}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Navigation error: {e}")
            return False

    def extract_all_cookies_comprehensive(self):
        """Extract ALL cookies using multiple methods"""
        try:
            logger.debug("🔍 Comprehensive cookie extraction: JavaScript + Selenium API")
            all_cookies = []
            
            cookies_script = """
            try {
                console.log('=== EXTRACTING COOKIES VIA JAVASCRIPT ===');
                let cookies = [];
                const cookieString = document.cookie;
                console.log('document.cookie:', cookieString);
                
                if (cookieString) {
                    const cookiePairs = cookieString.split(';');
                    for (let pair of cookiePairs) {
                        const [name, ...valueParts] = pair.trim().split('=');
                        const value = valueParts.join('=');
                        if (name && value) {
                            cookies.push({
                                name: name.trim(),
                                value: value.trim(),
                                domain: window.location.hostname,
                                path: '/',
                                secure: window.location.protocol === 'https:',
                                httpOnly: false
                            });
                        }
                    }
                }
                
                console.log('Found', cookies.length, 'cookies via document.cookie');
                return {
                    success: true,
                    cookies: cookies,
                    domain: window.location.hostname
                };
            } catch (e) {
                console.error('Error extracting cookies via JS:', e);
                return {
                    success: false,
                    error: 'JS Cookie extraction error: ' + e.message
                };
            }
            """
            
            js_result = self.driver.execute_script(cookies_script)
            
            if js_result and js_result.get('success'):
                js_cookies = js_result.get('cookies', [])
                logger.debug(f"✅ Extracted {len(js_cookies)} cookies via JavaScript")
                all_cookies.extend(js_cookies)
            
            try:
                selenium_cookies = self.driver.get_cookies()
                logger.debug(f"✅ Extracted {len(selenium_cookies)} cookies via Selenium API")
                
                for selenium_cookie in selenium_cookies:
                    cookie_name = selenium_cookie.get('name')
                    
                    if not any(c.get('name') == cookie_name for c in all_cookies):
                        all_cookies.append({
                            'name': cookie_name,
                            'value': selenium_cookie.get('value', ''),
                            'domain': selenium_cookie.get('domain', ''),
                            'path': selenium_cookie.get('path', '/'),
                            'secure': selenium_cookie.get('secure', True),
                            'httpOnly': selenium_cookie.get('httpOnly', False)
                        })
            
            except Exception as e:
                logger.error(f"❌ Error getting Selenium cookies: {e}")
            
            return all_cookies
            
        except Exception as e:
            logger.error(f"❌ Error in comprehensive cookie extraction: {e}")
            return []

    def extract_all_data_from_browser(self):
        """Extract ALL authentication data from browser"""
        try:
            logger.info("🔍 COMPREHENSIVE authentication data extraction...")
            time.sleep(6)
            
            logger.info("🌐 Navigating to reddit.com first to get authentication cookies...")
            self.driver.get("https://www.reddit.com")
            time.sleep(3)
            
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            logger.info("🍪 Extracting cookies from reddit.com...")
            reddit_cookies = self.extract_all_cookies_comprehensive()
            
            logger.info("🌐 Navigating to chat.reddit.com for Matrix token...")
            self.driver.get("https://chat.reddit.com")
            time.sleep(3)
            
            WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            token_script = """
            try {
                console.log('=== EXTRACTING MATRIX TOKEN ===');
                let matrixToken = null;
                
                for (let i = 0; i < localStorage.length; i++) {
                    let key = localStorage.key(i);
                    if (key === 'chat:matrix-access-token') {
                        matrixToken = localStorage.getItem(key);
                        console.log('FOUND matrix-access-token in localStorage');
                        break;
                    }
                }
                
                if (!matrixToken) {
                    for (let i = 0; i < localStorage.length; i++) {
                        let key = localStorage.key(i);
                        if (key && (key.includes('matrix') || key.includes('token'))) {
                            console.log('Checking key:', key);
                            let value = localStorage.getItem(key);
                            try {
                                let parsed = JSON.parse(value);
                                if (parsed && parsed.token) {
                                    matrixToken = parsed.token;
                                    console.log('Found token in JSON:', key);
                                    break;
                                }
                            } catch(e) {
                                if (value && value.startsWith('eyJ')) {
                                    matrixToken = value;
                                    console.log('Found JWT token in:', key);
                                    break;
                                }
                            }
                        }
                    }
                }
                
                if (matrixToken) {
                    console.log('Matrix token found, length:', matrixToken.length);
                    return {
                        success: true,
                        matrix_token: matrixToken,
                        token_source: 'localStorage'
                    };
                } else {
                    console.log('No matrix token found in localStorage')
                    return {
                        success: false,
                        error: 'No matrix token found'
                    };
                }
            } catch (e) {
                console.error('Error extracting matrix token:', e);
                return {
                    success: false,
                    error: 'Script error: ' + e.message
                };
            }
            """
            
            token_result = self.driver.execute_script(token_script)
            logger.info(f"📊 Token extraction result: {token_result}")
            
            matrix_token = None
            if token_result and token_result.get('success'):
                matrix_token = token_result.get('matrix_token')
                if matrix_token:
                    if matrix_token.startswith('"') and matrix_token.endswith('"'):
                        matrix_token = matrix_token[1:-1]
                        logger.info(f"✅ Stripped quotes from Matrix token")
                    logger.info(f"✅ Extracted Matrix token: {matrix_token[:30]}...")
                else:
                    logger.error("❌ Matrix token extraction returned success but no token")
            else:
                logger.error(f"❌ Failed to extract Matrix token: {token_result.get('error', 'Unknown error')}")
            
            logger.info("🍪 Extracting cookies from chat.reddit.com...")
            chat_cookies = self.extract_all_cookies_comprehensive()
            
            all_cookies = []
            seen_names = set()
            
            for cookie in reddit_cookies:
                if cookie['name'] not in seen_names:
                    all_cookies.append(cookie)
                    seen_names.add(cookie['name'])
            
            for cookie in chat_cookies:
                if cookie['name'] not in seen_names:
                    all_cookies.append(cookie)
                    seen_names.add(cookie['name'])
            
            important_cookies = ['reddit_session', 'token_v2', 'session', 'edge_session', 
                               'session_tracker', 'csrf_token', 'loid']
            found_important = []
            
            for cookie in all_cookies:
                if cookie['name'] in important_cookies:
                    found_important.append(cookie['name'])
                    logger.info(f"🔑 Found important cookie '{cookie['name']}': {cookie['value'][:20]}...")
            
            logger.info(f"📊 Total combined cookies from both domains: {len(all_cookies)}")
            logger.info(f"🔑 Found {len(found_important)} important cookies: {found_important}")
            
            if len(found_important) < 3:
                logger.warning("⚠️ Warning: Fewer than 3 important cookies found. Authentication may fail.")
            
            current_url = self.driver.current_url
            logger.info(f"📍 Browser is at: {current_url}")
            
            return {
                'matrix_token': matrix_token,
                'cookies': all_cookies,
                'current_url': current_url,
                'token_extraction_success': token_result.get('success', False) if token_result else False
            }
            
        except Exception as e:
            logger.error(f"❌ Error extracting comprehensive browser data: {e}")
            return {
                'matrix_token': None,
                'cookies': [],
                'current_url': None,
                'error': str(e)
            }

# ========== DATABASE CONVERSATION MANAGER ==========
class ConversationManager:
    """Manages conversation history and tracking in database"""
    
    def __init__(self, account_username: str):
        self.account_username = account_username
        logger.info(f"📊 Database Conversation Manager initialized for {account_username}")
    
    def mark_conversation_completed(self, user_id: str):
        """Mark a user's conversation as completed in database"""
        try:
            with db_manager.get_cursor() as cursor:
                # Use datetime.now() for timestamp columns
                current_time = datetime.now()
                cursor.execute("""
                    INSERT INTO completed_conversations (user_id, account_username, completed_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, account_username) DO NOTHING
                """, (user_id, self.account_username, current_time))
            
            logger.info(f"✅ [{self.account_username}] Marked conversation with user {user_id} as completed in database")
            return True
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error marking conversation completed: {e}")
            return False
    
    def is_conversation_completed(self, user_id: str) -> bool:
        """Check if we've completed a conversation with this user in database"""
        try:
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT 1 FROM completed_conversations 
                    WHERE user_id = %s AND account_username = %s
                    LIMIT 1
                """, (user_id, self.account_username))
                
                return cursor.fetchone() is not None
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking completed conversation: {e}")
            return False
            
       

class RedditMatrixChat:
    def __init__(self, account_username, proxy=None, adspower_profile_id=None):
        self.account_username = account_username
        self.proxy = proxy
        self.adspower_profile_id = adspower_profile_id
        self.session = requests.Session()
        self.direct_session = requests.Session()
        self.matrix_base = "https://matrix.redditspace.com/_matrix/client/v3"
        self.chat_base = "https://chat.reddit.com"
        self.reddit_base = "https://www.reddit.com"
        self.matrix_token = None
        self.session_cookies = []
        self.sync_filter = self._build_sync_filter()
        
        # FIXED: Pass self as matrix_chat_parent reference
        self.chat_interface = ChatInterfaceHandler(account_username, adspower_profile_id, self)
        
        if adspower_profile_id:
            self.adspower_browser = AdsPowerChatBrowser(base_url="http://local.adspower.net:50325")
        else:
            self.adspower_browser = None
        
        if self.adspower_browser and adspower_profile_id:
            self.adspower_browser.set_profile(adspower_profile_id)
        
        self.setup_sessions()
        self.next_batch = None
        self.persistent_state = PersistentStateManager(account_username)
        self.known_rooms = set()
        self.conversation_manager = ConversationManager(account_username)
        self.our_matrix_user_id = None
        self.image_sender = None
        self.initial_sync_complete = False
        self.image_analyzer = ImageAnalyzer()
        self.image_manager = ImageManager(account_username)
        self.direct_downloader = DirectIPDownloader()
        
        self.room_metadata = RoomMetadataManager(account_username)
        
        self.account_type = None
        self.room_count = 0
        self.initialization_phase = 0
        
        self.token_last_refreshed = 0
        self.token_refresh_attempts = 0
        self.max_token_age = 8 * 3600
        self.consecutive_failures = 0
        self.circuit_breaker_tripped = False
        self.circuit_breaker_reset_time = 0
        self.api_call_count = 0
        self.session_created_time = time.time()
        self.last_browser_data_extraction = 0
        self.browser_data_extraction_interval = 7200
        
        self.last_refresh_attempt = 0
        self.refresh_cooldown = 10
        self.refresh_attempt_times = []
        
        self.initial_room_check_complete = False
        self.background_init_started = False
        
        # Store pending messages from initial sync
        self.initial_sync_pending_messages = []
        self.initial_sync_processed = False
        
        # CRITICAL FIX: DO NOT create OutgoingMessenger here - it will be created by RedditChatBot
        # with the correct use_legacy_system flag
        self.outgoing_messenger = None  # Changed from OutgoingMessenger(...)
        
        logger.info(f"📡 [{self.account_username}] RedditMatrixChat initialized (OutgoingMessenger will be created later)")
        
    def _build_sync_filter(self):
        """Build the sync filter for optimized event streaming"""
        filter_config = {
            "room": {
                "timeline": {
                    "unread_thread_notifications": True,
                    "not_types": ["com.reddit.review_open", "com.reddit.review_close"],
                    "not_aggregated_relations": [
                        "m.annotation",
                        "com.reddit.hide_user_content", 
                        "com.reddit.potentially_toxic",
                        "com.reddit.display_settings",
                        "com.reddit.review_request"
                    ],
                    "lazy_load_members": True,
                    "limit": 50  # Get last 50 events per room
                },
                "state": {
                    "lazy_load_members": True
                },
                "ephemeral": {
                    "limit": 20
                }
            },
            "presence": {
                "limit": 0
            },
            "account_data": {
                "limit": 0
            }
        }
        return filter_config
    
    def setup_sessions(self):
        """Setup both proxy and direct IP sessions - FIXED VERSION"""
        # Create new session objects
        self.session = requests.Session()
        self.direct_session = requests.Session()
        
        # Clear any existing headers
        self.session.headers.clear()
        self.direct_session.headers.clear()
        
        # Setup proxy session headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Content-Type': 'application/json',
            'Origin': 'https://www.reddit.com',
            'Referer': 'https://www.reddit.com/chat',
        })
        
        # Setup direct session headers
        self.direct_session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36',
            'Accept': 'image/*,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        
        # Apply proxy configuration
        self._apply_proxy()
        
        logger.info(f"✅ [{self.account_username}] Sessions setup complete")
    
    def _apply_proxy(self):
        """Apply proxy to proxy session ONLY - FIXED VERSION"""
        try:
            if not self.proxy:
                self.session.proxies = {}
                logger.info(f"🔌 [{self.account_username}] No proxy configured")
                return

            p = self.proxy.strip()
            logger.info(f"🔌 [{self.account_username}] Applying proxy: {p[:50]}...")
            
            if p.startswith("socks://"):
                proxy_url = p
                self.session.proxies = {
                    'http': proxy_url, 
                    'https': proxy_url
                }
                logger.info(f"🔌 [{self.account_username}] Applied SOCKS5 proxy")
            elif p.startswith("socks5h://"):
                proxy_url = p
                self.session.proxies = {'http': proxy_url, 'https': proxy_url}
                logger.info(f"🔌 [{self.account_username}] Applied SOCKS5H proxy")
            elif p.startswith("http://"):
                proxy_url = p
                self.session.proxies = {'http': proxy_url, 'https': proxy_url}
                logger.info(f"🔌 [{self.account_username}] Applied HTTP proxy")
            elif p.startswith("https://"):
                proxy_url = p
                self.session.proxies = {'http': proxy_url, 'https': proxy_url}
                logger.info(f"🔌 [{self.account_username}] Applied HTTPS proxy")
            else:
                parts = p.split(':')
                if len(parts) == 2:
                    ip, port = parts
                    proxy_url = f"http://{ip}:{port}"
                    self.session.proxies = {'http': proxy_url, 'https': proxy_url}
                    logger.info(f"🔌 [{self.account_username}] Applied legacy HTTP proxy: {proxy_url}")
                elif len(parts) >= 4:
                    ip, port, username, password = parts[0], parts[1], parts[2], ':'.join(parts[3:])
                    proxy_url = f"http://{username}:{password}@{ip}:{port}"
                    self.session.proxies = {'http': proxy_url, 'https': proxy_url}
                    logger.info(f"🔌 [{self.account_username}] Applied legacy HTTP proxy with auth")
            
            # Test the proxy configuration
            logger.info(f"🔌 [{self.account_username}] Testing proxy configuration...")
            test_url = "https://httpbin.org/ip"
            
            try:
                test_response = self.session.get(test_url, timeout=10, verify=False)
                if test_response.status_code == 200:
                    logger.info(f"✅ [{self.account_username}] Proxy test successful")
                else:
                    logger.warning(f"⚠️ [{self.account_username}] Proxy test failed: {test_response.status_code}")
            except Exception as e:
                logger.warning(f"⚠️ [{self.account_username}] Proxy test error: {e}")
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Proxy setup error: {e}")
            self.session.proxies = {}

    def apply_cookies_from_browser(self, cookies: list):
        """Apply cookies extracted from browser to proxy session ONLY"""
        self.session.cookies.clear()
        count = 0
        
        for cookie in cookies:
            try:
                if not cookie.get('domain') or '.reddit.com' not in cookie['domain']:
                    continue
                    
                cookie_value = str(cookie.get('value', ''))
                if cookie_value.startswith('"') and cookie_value.endswith('"'):
                    cookie_value = cookie_value[1:-1]
                
                self.session.cookies.set(
                    name=cookie.get('name'),
                    value=cookie_value,
                    domain=cookie.get('domain'),
                    path=cookie.get('path', '/'),
                    secure=cookie.get('secure', True),
                    rest={'HttpOnly': cookie.get('httpOnly', False)}
                )
                count += 1
                logger.debug(f"🍪 Added cookie to proxy session: {cookie.get('name')}")
            except Exception as e:
                logger.debug(f"⚠️ Failed to add cookie {cookie.get('name')}: {e}")
        
        logger.info(f"🍪 [{self.account_username}] Applied {count} cookies from browser to proxy session")
        
        important_cookies = ['token_v2', 'csrf_token', 'session', 'reddit_session']
        for cookie_name in important_cookies:
            if cookie_name in self.session.cookies:
                value = self.session.cookies.get(cookie_name)
                if value:
                    logger.info(f"🔑 [{self.account_username}] Important cookie '{cookie_name}': {value[:20]}...")
    
    def can_attempt_refresh(self):
        """Check if we're allowed to attempt a refresh"""
        current_time = time.time()
        
        if self.circuit_breaker_tripped:
            if current_time < self.circuit_breaker_reset_time:
                logger.warning(f"🔴 [{self.account_username}] Circuit breaker active, skipping refresh")
                return False
            else:
                logger.info(f"🟢 [{self.account_username}] Circuit breaker reset")
                self.circuit_breaker_tripped = False
        
        if current_time - self.last_refresh_attempt < self.refresh_cooldown:
            logger.debug(f"⏳ [{self.account_username}] In refresh cooldown period")
            return False
        
        MAX_REFRESH_ATTEMPTS_PER_HOUR = 10
        REFRESH_ATTEMPT_WINDOW = 3600
        
        recent_attempts = [t for t in self.refresh_attempt_times 
                          if current_time - t < REFRESH_ATTEMPT_WINDOW]
        
        if len(recent_attempts) >= MAX_REFRESH_ATTEMPTS_PER_HOUR:
            logger.warning(f"⏰ [{self.account_username}] Hourly refresh limit reached ({len(recent_attempts)}/{MAX_REFRESH_ATTEMPTS_PER_HOUR})")
            return False
        
        return True
    
    def kill_chrome_processes(self):
        """Force kill any orphaned Chrome processes"""
        try:
            import psutil
            import os
            killed = 0
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                        os.kill(proc.info['pid'], 9)
                        killed += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied, ProcessLookupError):
                    pass
            if killed > 0:
                logger.info(f"🧹 [{self.account_username}] Killed {killed} orphaned Chrome processes")
        except ImportError:
            logger.warning(f"⚠️ [{self.account_username}] psutil not installed, skipping Chrome cleanup")
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Chrome cleanup error: {e}")
    
    def check_browser_state(self) -> bool:
        """Check if AdsPower browser is actually ready"""
        try:
            if not self.adspower_browser or not self.adspower_browser.driver:
                logger.info(f"🖥️ [{self.account_username}] Browser not connected, attempting to connect...")
                return self.adspower_browser.start_browser()
            
            try:
                current_url = self.adspower_browser.driver.current_url
                logger.debug(f"📍 [{self.account_username}] Browser is at: {current_url}")
                
                if "chat.reddit.com" not in current_url:
                    logger.warning(f"⚠️ [{self.account_username}] Browser not on chat.reddit.com, navigating...")
                    return self.adspower_browser.navigate_to_chat()
                
                return True
            except Exception as e:
                logger.warning(f"⚠️ [{self.account_username}] Browser check error: {e}, reconnecting...")
                self.adspower_browser.driver = None
                return self.adspower_browser.start_browser()
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Browser state check failed: {e}")
            return False
    
    def is_token_valid(self):
        """Check if Matrix token is still valid"""
        if not self.matrix_token:
            logger.debug(f"🔴 [{self.account_username}] No Matrix token available")
            return False
        
        if not self.matrix_token.startswith('eyJ'):
            logger.debug(f"🔴 [{self.account_username}] Token format invalid")
            return False
        
        # NEW: Quick sanity check - if token is very old, assume it's invalid
        if self.token_last_refreshed and (time.time() - self.token_last_refreshed) > 3600:  # 1 hour
            logger.debug(f"🕐 [{self.account_username}] Token is over 1 hour old, assuming expired")
            return False
        
        for attempt in range(2):
            try:
                url = f"{self.matrix_base}/account/whoami"
                headers = {
                    'Authorization': f'Bearer {self.matrix_token}',
                    'Accept': 'application/json'
                }
                
                logger.debug(f"🔍 [{self.account_username}] Testing token validity (attempt {attempt+1})...")
                response = self.session.get(url, headers=headers, timeout=15, verify=False)
                
                if response and response.status_code == 200:
                    logger.debug(f"🟢 [{self.account_username}] Token is valid")
                    return True
                elif response and response.status_code == 429:
                    logger.warning(f"⚠️ [{self.account_username}] Rate limited during token check")
                    if attempt == 0:
                        time.sleep(5)
                        continue
                    else:
                        return True  # Assume token is valid but we're rate limited
                else:
                    if attempt == 0:
                        logger.debug(f"🔴 [{self.account_username}] Token check failed: {response.status_code if response else 'No response'}, will retry")
                        time.sleep(3)
                        continue
                    else:
                        logger.warning(f"⚠️ [{self.account_username}] Token validation failed after retry")
                        return False
                        
            except requests.exceptions.Timeout:
                logger.warning(f"⏳ [{self.account_username}] Token check timed out")
                if attempt == 0:
                    time.sleep(3)
                    continue
                else:
                    logger.warning(f"⚠️ [{self.account_username}] Token check timed out twice")
                    return False
            except Exception as e:
                if attempt == 0:
                    logger.debug(f"🔴 [{self.account_username}] Token validation error: {e}, will retry")
                    time.sleep(3)
                    continue
                else:
                    logger.warning(f"⚠️ [{self.account_username}] Token validation error after retry: {e}")
                    return False
        
        return False
    
    def ensure_valid_connection(self):
        """Ensure we have valid connections - DYNAMIC TOKEN UPDATING WITH DEBUGGING"""
        # For RECEIVING messages: update token from CURRENT page
        try:
            logger.debug(f"🔍 [{self.account_username}] ensure_valid_connection called")
            
            # Check if token exists and is valid
            if self.matrix_token and self.is_token_valid():
                logger.debug(f"✅ [{self.account_username}] Token is valid, no refresh needed")
                return True
            
            # Token is invalid or doesn't exist - need to refresh browser AND get new token
            logger.warning(f"⚠️ [{self.account_username}] Token invalid or missing, refreshing browser session...")
            
            # CRITICAL: Refresh the browser session first
            if not self.refresh_browser_session():
                logger.error(f"❌ [{self.account_username}] Failed to refresh browser session")
                return False
            
            # Now extract token from FRESH browser
            current_token = self.get_current_token_from_browser()
            
            if current_token:
                # Update our token with the CURRENT one
                old_token_preview = self.matrix_token[:30] if self.matrix_token else "None"
                new_token_preview = current_token[:30]
                
                if self.matrix_token != current_token:
                    logger.info(f"🔄 [{self.account_username}] Updating token after browser refresh")
                    logger.info(f"   📄 Old token: {old_token_preview}...")
                    logger.info(f"   📄 New token: {new_token_preview}...")
                    logger.info(f"   📄 Token length: {len(current_token)} chars")
                    
                    self.matrix_token = current_token
                    self.token_last_refreshed = time.time()
                    
                    # Update image sender with new token
                    if self.image_sender:
                        self.image_sender.matrix_token = current_token
                    
                    logger.debug(f"✅ [{self.account_username}] Token updated successfully")
                    return True
                else:
                    # Token is still the same (shouldn't happen after refresh)
                    logger.warning(f"⚠️ [{self.account_username}] Token unchanged after browser refresh - might still be invalid")
                    
                    # Force full connection refresh
                    return self.full_connection_refresh()
            else:
                logger.error(f"❌ [{self.account_username}] Could not extract current token from browser after refresh")
                
                # Try the old validation as fallback
                logger.debug(f"🔍 [{self.account_username}] Falling back to full connection refresh...")
                return self.full_connection_refresh()
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error ensuring connection: {e}")
            import traceback
            logger.error(f"❌ [{self.account_username}] Traceback: {traceback.format_exc()}")
            # Fallback to full refresh
            return self.full_connection_refresh()

    def upload_image_to_matrix(self, image_path: str):
        """Upload image to Matrix media repository and get MXC URL"""
        
        # 1. Check authentication token
        if not self.matrix_token:
            logger.error(f"❌ [{self.account_username}] No Matrix token available for upload")
            return None, None
        
        # 2. Get image dimensions (optional, for metadata)
        try:
            from PIL import Image
            with Image.open(image_path) as img:
                width, height = img.size
        except ImportError:
            width, height = 800, 600
        except Exception as e:
            logger.warning(f"⚠️ [{self.account_username}] Could not get image dimensions: {e}")
            width, height = 800, 600
        
        # 3. Get file info
        file_size = os.path.getsize(image_path)
        mimetype = mimetypes.guess_type(image_path)[0] or 'image/jpeg'
        filename = os.path.basename(image_path)
        
        # 4. Construct upload URL
        upload_url = f"https://matrix.redditspace.com/_matrix/media/v3/upload?filename={filename}"
        
        # 5. Set headers (CRITICAL: includes auth and CORS headers)
        headers = {
            'Authorization': f'Bearer {self.matrix_token}',
            'Content-Type': mimetype,
            'Origin': 'https://chat.reddit.com',
            'Referer': 'https://chat.reddit.com/',
        }
        
        # 6. Read image file
        try:
            with open(image_path, 'rb') as f:
                image_data = f.read()
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error reading image file: {e}")
            return None, None
        
        # 7. Make the upload request (retry 3 times)
        for attempt in range(3):
            try:
                response = self.session.post(
                    upload_url,
                    data=image_data,  # Raw binary image data
                    headers=headers,
                    timeout=60,
                    verify=False
                )
                
                # 8. Process response
                if response.status_code == 200:
                    upload_data = response.json()
                    mxc_url = upload_data.get('content_uri')  # e.g., "mxc://matrix.redditspace.com/abc123"
                    
                    if mxc_url:
                        # 9. Return MXC URL + image metadata
                        image_info = {
                            "w": width,
                            "h": height,
                            "mimetype": mimetype,
                            "size": file_size
                        }
                        logger.info(f"✅ [{self.account_username}] Image uploaded successfully! MXC URL: {mxc_url[:50]}...")
                        return mxc_url, image_info
                else:
                    logger.error(f"❌ [{self.account_username}] Image upload failed: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Image upload error on attempt {attempt + 1}: {e}")
                if attempt < 2:
                    time.sleep(20)
                continue
        
        logger.error(f"❌ [{self.account_username}] Image upload failed after 3 attempts")
        return None, None

    def test_proxy_connection(self):
        """Test if proxy is working"""
        try:
            test_url = "http://httpbin.org/ip"
            logger.info(f"🔍 [{self.account_username}] Testing proxy connection...")
            logger.info(f"🔍 [{self.account_username}] Proxy: {self.session.proxies}")
            
            try:
                response = self.session.get(test_url, timeout=10, verify=False)
                if response and response.status_code == 200:
                    logger.info(f"✅ [{self.account_username}] Proxy test PASSED")
                    logger.info(f"   🌐 Your IP: {response.json().get('origin', 'Unknown')}")
                    return True
                else:
                    logger.error(f"❌ [{self.account_username}] Proxy test FAILED: {response.status_code if response else 'No response'}")
                    return False
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Proxy test ERROR: {type(e).__name__}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Proxy test setup error: {e}")
            return False

    def send_image_message(self, room_id: str, mxc_url: str, image_info: dict = None):
        """Send image message to Matrix room using cached MXC URL"""
        
        # 1. Check auth
        if not self.matrix_token:
            logger.error(f"❌ [{self.account_username}] No Matrix token available for sending image")
            return False
        
        # 2. Prepare image metadata (if not provided, use defaults)
        if not image_info:
            image_info = {
                "w": 800,
                "h": 600,
                "mimetype": "image/jpeg",
                "size": 100000
            }
        
        # 3. Generate unique transaction ID
        txn_id = f"m{int(time.time() * 1000)}.1"
        
        # 4. Construct the send URL
        url = f"{self.matrix_base}/rooms/{room_id}/send/m.room.message/{txn_id}"
        
        # 5. Create the message payload
        payload = {
            "msgtype": "m.image",  # Matrix message type for images
            "body": "😘",  # Fallback text (shown if image doesn't load)
            "url": mxc_url,  # The MXC URL from upload
            "info": image_info  # Image metadata
        }
        
        # 6. Set headers
        headers = {
            'Authorization': f'Bearer {self.matrix_token}',
            'Content-Type': 'application/json'
        }
        
        # 7. Make the PUT request to send the message
        try:
            response = self.session.put(
                url,
                json=payload,
                headers=headers,
                timeout=30,
                verify=False
            )
            
            # 8. Check response
            if response.status_code == 200:
                data = response.json()
                event_id = data.get('event_id')  # Unique event ID for this message
                logger.info(f"✅ [{self.account_username}] Image sent successfully to room {room_id[:8]}")
                return True
            else:
                logger.error(f"❌ [{self.account_username}] Failed to send image: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error sending image message: {e}")
            return False

    def send_image_from_file(self, room_id: str, image_path: str):
        """Upload and send image from file - TWO-STEP PROCESS"""
        
        # STEP 1: Upload image to get MXC URL
        logger.info(f"📤 [{self.account_username}] Uploading image: {os.path.basename(image_path)}")
        mxc_url, image_info = self.upload_image_to_matrix(image_path)
        if not mxc_url:
            logger.error(f"❌ [{self.account_username}] Failed to upload image")
            return False, None
        
        # STEP 2: Send message with the MXC URL
        logger.info(f"💬 [{self.account_username}] Sending image to room {room_id[:8]}")
        success = self.send_image_message(room_id, mxc_url, image_info)
        
        if success:
            # Cache the MXC URL for future use
            image_filename = os.path.basename(image_path)
            image_lower = image_path.lower()
            
            # Determine image type from path and cache accordingly
            if hasattr(self, 'image_manager') and self.image_manager:
                if 'ass' in image_lower:
                    self.image_manager.url_cache.cache_url('ass', image_filename, mxc_url)
                elif 'tits' in image_lower:
                    self.image_manager.url_cache.cache_url('tits', image_filename, mxc_url)
                elif 'feet' in image_lower:
                    self.image_manager.url_cache.cache_url('feet', image_filename, mxc_url)
                elif 'selfie' in image_lower:
                    self.image_manager.url_cache.cache_url('selfie', image_filename, mxc_url)
                elif 'random' in image_lower:
                    self.image_manager.url_cache.cache_url('random', image_filename, mxc_url)
            
            return True, mxc_url
        else:
            return False, None

    def diagnose_session_issue(self):
        """Diagnose why session.get() returns None"""
        try:
            logger.info(f"🔍 [{self.account_username}] Diagnosing session issue...")
            
            # Check session attributes
            logger.info(f"🔍 [{self.account_username}] Session type: {type(self.session)}")
            logger.info(f"🔍 [{self.account_username}] Session has 'get' method: {hasattr(self.session, 'get')}")
            
            # Check proxies
            if hasattr(self.session, 'proxies'):
                logger.info(f"🔍 [{self.account_username}] Session proxies: {self.session.proxies}")
            else:
                logger.warning(f"⚠️ [{self.account_username}] Session has no 'proxies' attribute")
            
            # Check session configuration
            logger.info(f"🔍 [{self.account_username}] Session headers: {dict(self.session.headers)}")
            
            # Try a simple test request WITHOUT proxy
            test_url = "https://httpbin.org/get"
            logger.info(f"🔍 [{self.account_username}] Testing session with NO proxy to {test_url}")
            
            try:
                # Save current proxy
                original_proxies = getattr(self.session, 'proxies', None)
                
                # Remove proxy for test
                self.session.proxies = {}
                
                test_response = self.session.get(test_url, timeout=10, verify=False)
                logger.info(f"🔍 [{self.account_username}] NO proxy test: Status={test_response.status_code}")
                
                # Restore proxy
                if original_proxies:
                    self.session.proxies = original_proxies
                else:
                    self._apply_proxy()  # Re-apply from config
                    
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] NO proxy test failed: {e}")
                # Try to restore proxy anyway
                if hasattr(self, 'proxy'):
                    self._apply_proxy()
            
            # Try a simple test request WITH proxy
            logger.info(f"🔍 [{self.account_username}] Testing session WITH proxy...")
            try:
                test_response = self.session.get(test_url, timeout=10, verify=False)
                logger.info(f"🔍 [{self.account_username}] WITH proxy test: Status={test_response.status_code}")
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] WITH proxy test failed: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Diagnose session issue error: {e}")
            return False

    def full_connection_refresh(self):
        """Complete refresh of everything"""
        current_time = time.time()
        
        if not self.can_attempt_refresh():
            return False
        
        self.last_refresh_attempt = current_time
        self.refresh_attempt_times.append(current_time)
        if len(self.refresh_attempt_times) > 10:
            self.refresh_attempt_times = self.refresh_attempt_times[-10:]
        
        if not refresh_coordinator.request_refresh(self.account_username):
            logger.info(f"⏳ [{self.account_username}] Waiting for refresh slot...")
            time.sleep(30)
            return False
        
        try:
            logger.info(f"🔄 [{self.account_username}] Performing FULL browser data refresh")
            logger.info(f"🔍 [{self.account_username}] Starting refresh process. Token exists: {bool(self.matrix_token)}, Browser exists: {bool(self.adspower_browser)}")
            
            logger.info(f"🔍 [{self.account_username}] Step 1: Checking browser state...")
            if not self.check_browser_state():
                logger.error(f"❌ [{self.account_username}] Browser state check failed")
                refresh_coordinator.release_refresh_lock(self.account_username)
                return False
            
            # NEW: First try to refresh the browser session (simpler than restarting)
            logger.info(f"🔄 [{self.account_username}] Attempting browser session refresh...")
            if self.refresh_browser_session():
                # Extract fresh data from refreshed browser
                logger.info(f"🔍 [{self.account_username}] Step 2: Extracting authentication data from refreshed browser...")
                browser_data = self.adspower_browser.extract_all_data_from_browser()
                
                has_token = bool(browser_data and browser_data.get('matrix_token'))
                has_cookies = bool(browser_data and browser_data.get('cookies'))
                logger.info(f"📊 [{self.account_username}] Extracted - Token: {has_token}, Cookies: {has_cookies}")
                
                if browser_data and browser_data.get('matrix_token'):
                    matrix_token = browser_data['matrix_token']
                    if matrix_token and matrix_token.startswith('"') and matrix_token.endswith('"'):
                        matrix_token = matrix_token[1:-1]
                        logger.info(f"✅ [{self.account_username}] Stripped quotes from Matrix token")
                    
                    self.matrix_token = matrix_token
                    
                    if browser_data.get('cookies'):
                        self.apply_cookies_from_browser(browser_data['cookies'])
                    else:
                        logger.warning(f"⚠️ [{self.account_username}] No cookies extracted from browser")
                    
                    self.image_sender = ImageSender(self.matrix_token, self.session)
                    
                    self.token_last_refreshed = time.time()
                    self.last_browser_data_extraction = time.time()
                    self.token_refresh_attempts = 0
                    self.consecutive_failures = 0
                    self.circuit_breaker_tripped = False
                    
                    if not self.detect_our_matrix_user_id():
                        logger.warning(f"⚠️ [{self.account_username}] Could not detect Matrix user ID after refresh")
                    
                    logger.info(f"✅ [{self.account_username}] FULL browser data refresh complete (via session refresh)")
                    logger.info(f"   📊 Matrix token: {self.matrix_token[:30]}...")
                    logger.info(f"   📊 Cookies applied: {len(self.session.cookies)}")
                    
                    refresh_coordinator.release_refresh_lock(self.account_username)
                    return True
                else:
                    logger.warning(f"⚠️ [{self.account_username}] Browser session refresh didn't yield token, falling back to full restart...")
            
            # Fallback to the old full restart method if session refresh fails
            if self.adspower_browser and self.adspower_browser.driver:
                try:
                    self.adspower_browser.driver.quit()
                    logger.info(f"✅ [{self.account_username}] Closed old browser session")
                except Exception as e:
                    logger.warning(f"⚠️ [{self.account_username}] Error closing browser: {e}")
                finally:
                    self.adspower_browser.driver = None
            
            self.kill_chrome_processes()
            
            time.sleep(3)
            
            self.session = requests.Session()
            self.direct_session = requests.Session()
            self.setup_sessions()
            self.session_created_time = time.time()
            
            if not self.adspower_browser or not self.adspower_profile_id:
                logger.error(f"❌ [{self.account_username}] No AdsPower browser configured")
                refresh_coordinator.release_refresh_lock(self.account_username)
                return False
            
            logger.info(f"🚀 [{self.account_username}] Connecting to AdsPower browser...")
            if not self.adspower_browser.start_browser():
                logger.error(f"❌ [{self.account_username}] Failed to connect to AdsPower browser")
                refresh_coordinator.release_refresh_lock(self.account_username)
                return False
            
            logger.info(f"🌐 [{self.account_username}] Navigating to chat.reddit.com...")
            if not self.adspower_browser.navigate_to_chat():
                logger.error(f"❌ [{self.account_username}] Failed to navigate to chat.reddit.com")
                refresh_coordinator.release_refresh_lock(self.account_username)
                return False
            
            logger.info(f"🔍 [{self.account_username}] Step 2: Extracting authentication data...")
            browser_data = self.adspower_browser.extract_all_data_from_browser()
            
            has_token = bool(browser_data and browser_data.get('matrix_token'))
            has_cookies = bool(browser_data and browser_data.get('cookies'))
            logger.info(f"📊 [{self.account_username}] Extracted - Token: {has_token}, Cookies: {has_cookies}")
            
            if not browser_data or not browser_data.get('matrix_token'):
                logger.error(f"❌ [{self.account_username}] Failed to extract Matrix token from browser")
                refresh_coordinator.release_refresh_lock(self.account_username)
                return False
            
            matrix_token = browser_data['matrix_token']
            if matrix_token and matrix_token.startswith('"') and matrix_token.endswith('"'):
                matrix_token = matrix_token[1:-1]
                logger.info(f"✅ [{self.account_username}] Stripped quotes from Matrix token")
            
            self.matrix_token = matrix_token
            
            if browser_data.get('cookies'):
                self.apply_cookies_from_browser(browser_data['cookies'])
            else:
                logger.warning(f"⚠️ [{self.account_username}] No cookies extracted from browser")
            
            self.image_sender = ImageSender(self.matrix_token, self.session)
            
            self.token_last_refreshed = time.time()
            self.last_browser_data_extraction = time.time()
            self.token_refresh_attempts = 0
            self.consecutive_failures = 0
            self.circuit_breaker_tripped = False
            
            if not self.detect_our_matrix_user_id():
                logger.warning(f"⚠️ [{self.account_username}] Could not detect Matrix user ID after refresh")
            
            logger.info(f"✅ [{self.account_username}] FULL browser data refresh complete")
            logger.info(f"   📊 Matrix token: {self.matrix_token[:30]}...")
            logger.info(f"   📊 Cookies applied: {len(self.session.cookies)}")
            logger.info(f"   📊 Browser data age: 0 seconds")
            
            refresh_coordinator.release_refresh_lock(self.account_username)
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Full browser data refresh error: {e}")
            self.consecutive_failures += 1
            if self.consecutive_failures >= 3:
                self.circuit_breaker_tripped = True
                self.circuit_breaker_reset_time = time.time() + 1800
                logger.error(f"🔴 [{self.account_username}] Circuit breaker tripped for 30 minutes")
            
            refresh_coordinator.release_refresh_lock(self.account_username)
            return False

    def test_session_connectivity(self):
        """Test session connectivity to Matrix server"""
        try:
            logger.info(f"🔍 [{self.account_username}] Testing session connectivity...")
            
            # Test 1: Can we reach Matrix server at all?
            test_url = f"{self.matrix_base}/versions"
            
            try:
                response = self.session.get(test_url, timeout=10, verify=False)
                logger.info(f"🔍 [{self.account_username}] Matrix server reachable: {response.status_code}")
                if response.status_code == 200:
                    logger.info(f"✅ [{self.account_username}] Matrix server is up and responding")
                    return True
                else:
                    logger.warning(f"⚠️ [{self.account_username}] Matrix server returned non-200: {response.status_code}")
                    return False
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Cannot reach Matrix server: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Session connectivity test error: {e}")
            return False

    def get_current_token_from_browser(self) -> str:
        """Extract the CURRENT Matrix token from the CURRENT browser page"""
        try:
            if not self.adspower_browser or not self.adspower_browser.driver:
                return None
            
            # Extract token from CURRENT page
            token_script = """
            try {
                let matrixToken = null;
                
                // First check localStorage
                for (let i = 0; i < localStorage.length; i++) {
                    let key = localStorage.key(i);
                    if (key === 'chat:matrix-access-token') {
                        matrixToken = localStorage.getItem(key);
                        console.log('FOUND matrix-access-token in localStorage:', matrixToken?.length || 0, 'chars');
                        break;
                    }
                }
                
                if (!matrixToken) {
                    // Check for any token-like values
                    for (let i = 0; i < localStorage.length; i++) {
                        let key = localStorage.key(i);
                        if (key && (key.includes('matrix') || key.includes('token'))) {
                            let value = localStorage.getItem(key);
                            try {
                                let parsed = JSON.parse(value);
                                if (parsed && parsed.token) {
                                    matrixToken = parsed.token;
                                    console.log('Found token in JSON:', key);
                                    break;
                                }
                            } catch(e) {
                                if (value && value.startsWith('eyJ')) {
                                    matrixToken = value;
                                    console.log('Found JWT token in:', key);
                                    break;
                                }
                            }
                        }
                    }
                }
                
                return matrixToken ? matrixToken : null;
            } catch(e) {
                console.error('Error extracting token:', e);
                return null;
            }
            """
            
            token = self.adspower_browser.driver.execute_script(token_script)
            
            if token:
                if token.startswith('"') and token.endswith('"'):
                    token = token[1:-1]
                
                logger.info(f"🔑 [{self.account_username}] Extracted CURRENT token from browser: {token[:30]}...")
                return token
            
            return None
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error extracting current token: {e}")
            return None

    def refresh_browser_session(self):
        """Refresh the browser session to get a fresh Matrix token"""
        try:
            logger.info(f"🔄 [{self.account_username}] Refreshing browser session...")
            
            if not self.adspower_browser or not self.adspower_browser.driver:
                logger.error(f"❌ [{self.account_username}] No browser available for refresh")
                return False
            
            driver = self.adspower_browser.driver
            current_url = driver.current_url
            
            # Step 1: Navigate to reddit.com to refresh the session
            logger.info(f"🌐 [{self.account_username}] Step 1: Navigating to reddit.com to refresh session...")
            driver.get("https://www.reddit.com")
            
            # Wait for page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(3)  # Wait for session to refresh
            
            # Step 2: Navigate to chat.reddit.com to get fresh Matrix token
            logger.info(f"🌐 [{self.account_username}] Step 2: Navigating to chat.reddit.com for fresh token...")
            driver.get("https://chat.reddit.com")
            
            # Wait for page to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            time.sleep(5)  # IMPORTANT: Wait longer for Matrix token to load
            
            # Step 3: Check if we're on a valid chat URL
            new_url = driver.current_url
            valid_patterns = [
                "chat.reddit.com",
                "reddit.com/chat/room/",
                "reddit.com/chat/",
                "www.reddit.com/chat/"
            ]
            
            is_valid_chat_url = any(pattern in new_url for pattern in valid_patterns)
            
            if not is_valid_chat_url:
                logger.error(f"❌ [{self.account_username}] Failed to navigate to valid chat URL after refresh: {new_url}")
                return False
            
            logger.info(f"✅ [{self.account_username}] Browser session refreshed successfully")
            logger.info(f"   📍 New URL: {new_url}")
            
            # Clear any old cookies/cache by forcing a fresh extraction
            if hasattr(self, 'session_cookies'):
                self.session_cookies = []
            
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error refreshing browser session: {e}")
            import traceback
            logger.error(f"❌ [{self.account_username}] Traceback: {traceback.format_exc()}")
            return False

    def _should_check_token(self) -> bool:
        """Determine if we should check token validity - DISABLED for chat interface"""
        # NEVER check token when using chat interface
        return False

    def send_message(self, room_id: str, message: str) -> bool:
        """Send message via chat interface - NO TOKEN CHECKS, JUST NAVIGATION"""
        try:
            logger.info(f"💬 [{self.account_username}] Sending message to room {room_id[:8]}")
            
            # NO TOKEN CHECKS - Browser doesn't need token for sending!
            
            # Navigate to the room
            if not self.chat_interface.navigate_to_room(room_id):
                logger.error(f"❌ [{self.account_username}] Failed to navigate to room {room_id[:8]}")
                return False
            
            # After navigation, update our token from the new page (for future syncs)
            # But DON'T block message sending on this!
            threading.Thread(
                target=self._update_token_after_navigation,
                name=f"{self.account_username}_token_update"
            ).start()
            
            # Send the message
            success = self.chat_interface.send_message_to_current_room(message)
            
            if success:
                timestamp = int(time.time() * 1000)
                self.persistent_state.update_last_reply_timestamp(room_id, timestamp)
                logger.debug(f"📝 [{self.account_username}] Updated last reply timestamp")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error sending message: {e}")
            return False
    
    def _update_token_after_navigation(self):
        """Update token after navigation (async, non-blocking)"""
        try:
            time.sleep(1)  # Wait a moment for page to load
            current_token = self.get_current_token_from_browser()
            if current_token:
                self.matrix_token = current_token
                logger.debug(f"🔄 [{self.account_username}] Async token update after navigation")
        except:
            pass  # Silent fail - token update is nice-to-have

    def initialize_from_browser(self):
        """Initialize everything from browser data - MODIFIED TO NOT CREATE OUTGOING MESSENGER"""
        browser_data = self.get_browser_data()
        if not browser_data or not browser_data.get('matrix_token'):
            logger.error(f"❌ [{self.account_username}] Failed to get browser data from browser")
            return False
        
        matrix_token = browser_data['matrix_token']
        if matrix_token and matrix_token.startswith('"') and matrix_token.endswith('"'):
            matrix_token = matrix_token[1:-1]
            logger.info(f"✅ [{self.account_username}] Stripped quotes from Matrix token")
        
        self.matrix_token = matrix_token
        
        if browser_data.get('cookies'):
            self.apply_cookies_from_browser(browser_data['cookies'])
        else:
            logger.warning(f"⚠️ [{self.account_username}] No cookies extracted from browser")
        
        self.image_sender = ImageSender(self.matrix_token, self.session)
        self.token_last_refreshed = time.time()
        self.last_browser_data_extraction = time.time()
        
        # DO NOT CREATE OUTGOING MESSENGER - OUTGOING MESSAGES DISABLED
        logger.info(f"📭 [{self.account_username}] Outgoing messenger NOT created (outgoing messages disabled)")
        
        # NEW: Recover sent images from existing conversations
        logger.info(f"📁 [{self.account_username}] Recovering sent images from existing conversations...")
        if hasattr(self, 'image_manager') and self.image_manager:
            self.image_manager.recover_sent_images_from_existing_conversations()
            logger.info(f"✅ [{self.account_username}] Recovered sent images from database")
        
        logger.info(f"✅ [{self.account_username}] Initialized from browser (outgoing messages disabled)")
        return True

    def send_message(self, room_id: str, message: str) -> bool:
        """Send message via chat interface navigation"""
        try:
            logger.info(f"💬 [{self.account_username}] Sending message to room {room_id[:8]} via chat interface")
            
            # Navigate to the room
            if not self.chat_interface.navigate_to_room(room_id):
                logger.error(f"❌ [{self.account_username}] Failed to navigate to room {room_id[:8]}")
                return False
            
            # Send the message
            success = self.chat_interface.send_message_to_current_room(message)
            
            if success:
                # Update last reply timestamp in database
                timestamp = int(time.time() * 1000)
                self.persistent_state.update_last_reply_timestamp(room_id, timestamp)
                logger.debug(f"📝 [{self.account_username}] Updated last reply timestamp for room {room_id[:8]} to {timestamp}")
            
            return success
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error sending message via chat interface: {e}")
            return False

 
    def send_image(self, room_id: str, image_path_or_url: str, is_cached_url: bool = False):
        """Public send_image method - NOW USES API COMPLETELY"""
        return self._send_image_safe(room_id, image_path_or_url, is_cached_url)

    def _send_image_safe(self, room_id: str, image_path_or_url: str, is_cached_url: bool = False):
        """Safe wrapper for send_image with caching"""
        
        # 1. Ensure connection is valid
        if not self.ensure_valid_connection():
            return False
        
        try:
            if is_cached_url:
                # Already have MXC URL, just send it
                logger.info(f"🔄 [{self.account_username}] Using cached MXC URL: {image_path_or_url[:50]}...")
                success = self.send_image_message(room_id, image_path_or_url)
            else:
                # Need to upload first
                logger.info(f"📤 [{self.account_username}] Uploading and sending image: {os.path.basename(image_path_or_url)}")
                success, mxc_url = self.send_image_from_file(room_id, image_path_or_url)
                
                # 2. Cache the MXC URL for future use
                if success and mxc_url:
                    image_filename = os.path.basename(image_path_or_url)
                    
                    # Determine image type from path and cache accordingly
                    if hasattr(self, 'image_manager') and self.image_manager:
                        image_lower = image_path_or_url.lower()
                        if 'ass' in image_lower:
                            self.image_manager.url_cache.cache_url('ass', image_filename, mxc_url)
                        elif 'tits' in image_lower:
                            self.image_manager.url_cache.cache_url('tits', image_filename, mxc_url)
                        elif 'feet' in image_lower:
                            self.image_manager.url_cache.cache_url('feet', image_filename, mxc_url)
                        elif 'selfie' in image_lower:
                            self.image_manager.url_cache.cache_url('selfie', image_filename, mxc_url)
                        elif 'random' in image_lower:
                            self.image_manager.url_cache.cache_url('random', image_filename, mxc_url)
            
            # 3. Update conversation timestamp
            if success:
                timestamp = int(time.time() * 1000)
                self.persistent_state.update_last_reply_timestamp(room_id, timestamp)
                logger.info(f"✅ [{self.account_username}] Image sent successfully")
            
            return success
                    
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Image send error: {e}")
            return False

    def cleanup(self):
        """Clean up all resources"""
        try:
            if self.chat_interface:
                self.chat_interface.cleanup()
            logger.info(f"🧹 [{self.account_username}] Cleaned up all resources")
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error during cleanup: {e}")            

    def detect_our_matrix_user_id(self):
        """Dynamically detect our Matrix user ID from sync data"""
        try:
            if not self.matrix_token:
                logger.error(f"❌ [{self.account_username}] Cannot detect Matrix user ID: No Matrix token")
                return False
            
            url = f"{self.matrix_base}/account/whoami"
            headers = {
                'Authorization': f'Bearer {self.matrix_token}',
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
            
            logger.info(f"🔍 [{self.account_username}] Detecting Matrix user ID...")
            response = self.session.get(url, headers=headers, timeout=30, verify=False)
            
            if response and response.status_code == 200:
                data = response.json()
                user_id = data.get('user_id')
                if user_id:
                    self.our_matrix_user_id = user_id
                    logger.info(f"✅ [{self.account_username}] Detected our Matrix user ID from whoami: {self.our_matrix_user_id}")
                    return True
                else:
                    logger.error(f"❌ [{self.account_username}] whoami endpoint returned no user_id")
            else:
                if response:
                    logger.error(f"❌ [{self.account_username}] whoami request failed: {response.status_code}")
                else:
                    logger.error(f"❌ [{self.account_username}] whoami request failed: No response")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error detecting Matrix user ID: {e}")
            return False

    def test_reddit_auth(self):
        """Test basic Reddit authentication with browser cookies"""
        try:
            url = f"{self.reddit_base}/api/v1/me"
            
            logger.info(f"🔍 [{self.account_username}] Testing Reddit auth with {len(self.session.cookies)} cookies:")
            important_cookies = ['reddit_session', 'token_v2', 'session', 'edge_session', 
                               'session_tracker', 'csrf_token', 'loid']
            
            for cookie in self.session.cookies:
                cookie_value_preview = cookie.value[:30] if cookie.value else "EMPTY"
                if cookie.name in important_cookies:
                    logger.info(f"   🔑 {cookie.name}: {cookie_value_preview}...")
                else:
                    logger.debug(f"   🍪 {cookie.name}: {cookie_value_preview}...")
            
            found_important = sum(1 for cookie in self.session.cookies if cookie.name in important_cookies)
            logger.info(f"📊 [{self.account_username}] Found {found_important}/{len(important_cookies)} important cookies")
            
            logger.info(f"🔍 [{self.account_username}] Testing Reddit auth with browser cookies...")
            response = self.session.get(url, timeout=30, verify=False)
            
            if response and response.status_code == 200:
                user_data = response.json()
                username = user_data.get('name', 'Unknown')
                logger.info(f"✅ [{self.account_username}] Reddit auth successful - User: {username}")
                return True
            else:
                logger.error(f"❌ [{self.account_username}] Reddit auth test failed: {response.status_code if response else 'No response'}")
                if response:
                    logger.error(f"❌ [{self.account_username}] Response text: {response.text[:200]}")
                
                missing_important = [name for name in important_cookies 
                                   if name not in [c.name for c in self.session.cookies]]
                if missing_important:
                    logger.warning(f"⚠️ [{self.account_username}] Missing important cookies: {missing_important}")
                
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Reddit auth test error: {e}")
            return False

    def test_matrix_api(self):
        """Test Matrix API connectivity with detailed debugging"""
        try:
            if not self.matrix_token:
                logger.error(f"❌ [{self.account_username}] No Matrix token for API test")
                return False
            
            # Test 1: Simple whoami endpoint
            url = f"{self.matrix_base}/account/whoami"
            headers = {
                'Authorization': f'Bearer {self.matrix_token}',
                'Accept': 'application/json'
            }
            
            logger.info(f"🔍 [{self.account_username}] Testing Matrix API connectivity...")
            logger.info(f"🔍 [{self.account_username}] URL: {url}")
            logger.info(f"🔍 [{self.account_username}] Token preview: {self.matrix_token[:30]}...")
            
            try:
                response = self.session.get(url, headers=headers, timeout=15, verify=False)
                
                if response:
                    logger.info(f"📊 [{self.account_username}] Response status: {response.status_code}")
                    logger.info(f"📊 [{self.account_username}] Response time: {response.elapsed.total_seconds():.2f}s")
                    
                    if response.status_code == 200:
                        data = response.json()
                        logger.info(f"✅ [{self.account_username}] Matrix API test PASSED")
                        logger.info(f"   👤 User ID: {data.get('user_id', 'Unknown')}")
                        return True
                    else:
                        logger.error(f"❌ [{self.account_username}] Matrix API test FAILED: {response.status_code}")
                        logger.error(f"❌ [{self.account_username}] Response: {response.text[:200] if response.text else 'Empty'}")
                        return False
                else:
                    logger.error(f"❌ [{self.account_username}] Matrix API test NO RESPONSE")
                    return False
                    
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Matrix API test EXCEPTION: {type(e).__name__}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Matrix API test setup error: {e}")
            return False

    def test_connection(self):
        """Test connection to Reddit Matrix - MODIFIED TO NOT SEND INITIAL OUTGOING"""
        logger.info(f"🔍 [{self.account_username}] Testing connection...")
        
        try:
            logger.info(f"🔍 [{self.account_username}] Step 1: Initializing from browser...")
            if not self.initialize_from_browser():
                logger.error(f"❌ [{self.account_username}] Step 1 FAILED: browser initialization")
                return False
            logger.info(f"✅ [{self.account_username}] Step 1 PASSED: browser initialization")
            
            logger.info(f"🔍 [{self.account_username}] Step 2: Testing Reddit authentication...")
            if not self.test_reddit_auth():
                logger.error(f"❌ [{self.account_username}] Step 2 FAILED: Reddit authentication")
                return False
            logger.info(f"✅ [{self.account_username}] Step 2 PASSED: Reddit authentication")
            
            logger.info(f"🔍 [{self.account_username}] Step 3: Detecting Matrix user ID...")
            if not self.detect_our_matrix_user_id():
                logger.error(f"❌ [{self.account_username}] Step 3 FAILED: Matrix user ID detection")
                return False
            logger.info(f"✅ [{self.account_username}] Step 3 PASSED: Matrix user ID: {self.our_matrix_user_id}")
            
            logger.info(f"🔍 [{self.account_username}] Step 4: Performing sync initialization...")
            if not self.perform_sync_initialization():
                logger.error(f"❌ [{self.account_username}] Step 4 FAILED: Sync initialization")
                return False
            logger.info(f"✅ [{self.account_username}] Step 4 PASSED: Sync initialization complete")
            
            # DO NOT SEND INITIAL OUTGOING MESSAGE - OUTGOING MESSAGES DISABLED
            logger.info(f"📭 [{self.account_username}] Initial outgoing message NOT sent (outgoing messages disabled)")
            
            return True
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Connection test error: {e}")
            return False


    def perform_sync_initialization(self):
        """Perform initial sync to get current state"""
        try:
            logger.info(f"🔄 [{self.account_username}] Performing initial sync...")
            
            # Clear next_batch to get ALL events
            original_next_batch = self.next_batch
            self.next_batch = None
            
            initial_sync = self.sync(timeout=0)
            
            # Restore next_batch
            self.next_batch = original_next_batch
            
            if not initial_sync:
                logger.warning(f"⚠️ [{self.account_username}] Initial sync returned empty")
                return True
            
            joined_rooms = initial_sync.get('rooms', {}).get('join', {})
            invite_rooms = initial_sync.get('rooms', {}).get('invite', {})
            
            self.room_count = len(joined_rooms)
            logger.info(f"📊 [{self.account_username}] Initial sync found {self.room_count} joined rooms, {len(invite_rooms)} invited rooms")
            
            # JOIN INVITES IMMEDIATELY
            for room_id in invite_rooms.keys():
                logger.info(f"🚪 [{self.account_username}] Joining invited room from initial sync: {room_id[:8]}")
                if self.join_room(room_id):
                    self.known_rooms.add(room_id)
                    logger.info(f"✅ [{self.account_username}] Joined invited room {room_id[:8]}")
                else:
                    logger.warning(f"⚠️ [{self.account_username}] Failed to join invited room {room_id[:8]}")
            
            # CHECK EXISTING CONVERSATIONS - CRITICAL FIX: COLLECT PENDING MESSAGES
            logger.info(f"🔍 [{self.account_username}] Checking existing conversations from {len(joined_rooms)} joined rooms...")
            for room_id, room_data in joined_rooms.items():
                self.known_rooms.add(room_id)
                
                events = room_data.get('timeline', {}).get('events', [])
                if events:
                    # Update metadata - this saves directly to database
                    self.room_metadata.update_room_metadata_from_sync(room_id, events)
                    logger.debug(f"📥 [{self.account_username}] Found {len(events)} events in room {room_id[:8]} from initial sync")
                    
                    # Check if we have conversation state for this room in database
                    try:
                        with db_manager.get_cursor() as cursor:
                            cursor.execute("""
                                SELECT 1 FROM conversation_states 
                                WHERE room_id = %s AND account_username = %s
                                LIMIT 1
                            """, (room_id, self.account_username))
                            
                            has_state = cursor.fetchone() is not None
                            
                            if has_state:
                                logger.info(f"📝 [{self.account_username}] Room {room_id[:8]} has existing conversation state, checking for pending messages...")
                                # Collect pending messages from initial sync
                                self.collect_pending_messages_from_initial_sync(room_id, events)
                            
                    except Exception as e:
                        logger.error(f"❌ [{self.account_username}] Error checking conversation state for room {room_id[:8]}: {e}")
            
            # No need to call save_metadata() - it's already saved in update_room_metadata_from_sync()
            self.initial_room_check_complete = True
            
            logger.info(f"✅ [{self.account_username}] Sync initialization complete. Found {len(self.initial_sync_pending_messages)} pending messages from initial sync.")
            return True
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Sync initialization error: {e}")
            return False
    
    def collect_pending_messages_from_initial_sync(self, room_id: str, events: list):
        """Collect pending messages from initial sync for later processing"""
        try:
            if not self.our_matrix_user_id:
                return
            
            # Get room name from state events
            room_name = f"Room_{room_id[:8]}"
            for event in events:
                if event.get('type') == 'm.room.name':
                    room_name = event.get('content', {}).get('name', room_name)
            
            # Get conversation state - use the load_conversation_state method
            conversation_state = self.persistent_state.load_conversation_state(room_id)
            message_count = conversation_state.get('message_count', 0)
            
            # Check if we need to respond
            last_reply_timestamp = self.persistent_state.get_last_reply_timestamp(room_id)
            
            # Get the most recent user messages
            user_messages = []
            for event in events:
                if (event.get('type') == 'm.room.message' and 
                    not self.is_our_message(event.get('sender', ''))):
                    
                    timestamp = event.get('origin_server_ts', 0)
                    message_id = event.get('event_id', '')
                    
                    # Check if message is after our last reply AND not already processed
                    if timestamp > last_reply_timestamp and not self.persistent_state.is_message_processed(message_id):
                        content = event.get('content', {})
                        body = content.get('body', '').strip()
                        
                        if body:
                            username = self.extract_reddit_username(event.get('sender', ''))
                            user_messages.append({
                                'room_id': room_id,
                                'room_name': room_name,
                                'sender': event.get('sender', ''),
                                'username': username,
                                'message': body,
                                'message_id': message_id,
                                'timestamp': timestamp,
                                'is_image': False
                            })
            
            if user_messages:
                logger.info(f"📬 [{self.account_username}] Collected {len(user_messages)} pending messages from initial sync for room {room_id[:8]}")
                self.initial_sync_pending_messages.extend(user_messages)
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error collecting pending messages for room {room_id[:8]}: {e}")

    def is_mod_ban_message(self, message_content: str, sender: str = None) -> bool:
        """
        Detect if a message is a ban notice from Reddit moderators.
        """
        if not message_content:
            return False
        
        content_lower = message_content.lower()
        
        if sender:
            sender_lower = sender.lower()
            if "mod" in sender_lower or "moderator" in sender_lower:
                return True
        
        ban_phrases = [
            "you have been permanently banned",
            "you have been banned from participating",
            "permanent ban from",
            "banned from r/",
            "because you broke this community's rules",
            "won't be able to post or comment",
            "can still view and subscribe",
            "contact the moderator team by replying",
            "you have been temporarily banned",
            "temporary ban from",
            "your post has been removed",
            "your comment has been removed",
            "moderator of r/",
            "community rules violation",
        ]
        
        for phrase in ban_phrases:
            if phrase in content_lower:
                return True
        
        subreddit_pattern = r"r/\w+\s*$"
        if re.search(subreddit_pattern, content_lower):
            if content_lower.startswith("hello,"):
                return True
        
        return False
    
    def extract_banned_subreddit(self, message_content: str) -> str:
        """Extract the subreddit name from a ban message"""
        if not self.is_mod_ban_message(message_content):
            return None
        
        patterns = [
            r"banned from (r/\w+)",
            r"participating in (r/\w+)",
            r"r/(\w+)\s+MOD",
            r"moderator of (r/\w+)",
        ]
        
        for pattern in patterns:
            match = re.search(pattern, message_content.lower())
            if match:
                return match.group(1)
        
        return None
    
    def handle_mod_message(self, room_id: str, user_id: str, message_content: str):
        """Handle mod/ban messages by marking conversation as completed"""
        banned_subreddit = self.extract_banned_subreddit(message_content)
        
        if banned_subreddit:
            logger.info(f"🚫 [{self.account_username}] Received ban from {banned_subreddit}, marking conversation as completed")
        else:
            logger.info(f"🚫 [{self.account_username}] Received mod message, marking conversation as completed")
        
        self.conversation_manager.mark_conversation_completed(user_id)

    # ========== OPTIMIZED SYNC-BASED MESSAGE PROCESSING ==========
    def _sync_raw(self, timeout=5000):
        """Raw sync method - SINGLE source of truth for new messages - FIXED TOKEN HANDLING"""
        try:
            if not self.matrix_token:
                logger.debug(f"🔴 [{self.account_username}] No Matrix token for sync")
                return {}
            
            # NEW: Proactively check if token is valid before sync attempt
            if not self.is_token_valid():
                logger.warning(f"⚠️ [{self.account_username}] Token invalid before sync, attempting refresh...")
                if not self.ensure_valid_connection():
                    logger.error(f"❌ [{self.account_username}] Failed to refresh token before sync")
                    return {}
            
            # DEBUG: Log token info
            token_preview = self.matrix_token[:30] if self.matrix_token else "None"
            logger.debug(f"🔍 [{self.account_username}] Token preview: {token_preview}...")
            logger.debug(f"🔍 [{self.account_username}] Token length: {len(self.matrix_token) if self.matrix_token else 0}")
            
            params = {
                'filter': json.dumps(self.sync_filter),
                'timeout': timeout,
                'streaming': 'false'
            }
            
            if self.next_batch:
                params['since'] = self.next_batch
                logger.debug(f"🔍 [{self.account_username}] Using next_batch: {self.next_batch[:50]}...")
            else:
                logger.debug(f"🔍 [{self.account_username}] No next_batch, getting initial sync")
            
            headers = {
                'Authorization': f'Bearer {self.matrix_token}',
                'Accept': 'application/json'
            }
            
            url = f"{self.matrix_base}/sync"
            
            # DEBUG: Log full request details
            logger.debug(f"🔍 [{self.account_username}] Full sync URL: {url}")
            logger.debug(f"🔍 [{self.account_username}] Headers: Authorization={headers['Authorization'][:30]}..., Accept={headers['Accept']}")
            logger.debug(f"🔍 [{self.account_username}] Params: {params}")
            
            # Check session proxy
            if hasattr(self.session, 'proxies') and self.session.proxies:
                proxy_info = self.session.proxies.get('https') or self.session.proxies.get('http')
                logger.debug(f"🔍 [{self.account_username}] Using proxy: {proxy_info}")
            else:
                logger.debug(f"🔍 [{self.account_username}] No proxy configured for session")
            
            start_time = time.time()
            logger.debug(f"📡 [{self.account_username}] Making sync request...")
            
            try:
                response = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=(timeout/1000) + 15,
                    verify=False
                )
            except requests.exceptions.Timeout as e:
                logger.error(f"⏳ [{self.account_username}] Sync TIMEOUT after {(timeout/1000) + 15}s: {e}")
                return {}
            except requests.exceptions.ConnectionError as e:
                logger.error(f"🔌 [{self.account_username}] Sync CONNECTION ERROR: {e}")
                return {}
            except requests.exceptions.RequestException as e:
                logger.error(f"⚠️ [{self.account_username}] Sync REQUEST EXCEPTION: {type(e).__name__}: {e}")
                return {}
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Sync UNEXPECTED ERROR: {type(e).__name__}: {e}")
                import traceback
                logger.error(f"❌ [{self.account_username}] Traceback: {traceback.format_exc()}")
                return {}
            
            elapsed_time = time.time() - start_time
            logger.debug(f"📊 [{self.account_username}] Sync response received in {elapsed_time:.2f}s")
            
            if response is None:
                logger.error(f"❌ [{self.account_username}] Sync failed: response is None (session.get() returned None)")
                return {}
            
            logger.debug(f"📊 [{self.account_username}] Response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    self.next_batch = data.get('next_batch')
                    
                    # DEBUG: Log sync results
                    joined_rooms = data.get('rooms', {}).get('join', {})
                    invite_rooms = data.get('rooms', {}).get('invite', {})
                    logger.debug(f"📊 [{self.account_username}] Sync success: {len(joined_rooms)} joined rooms, {len(invite_rooms)} invites")
                    
                    return data
                    
                except json.JSONDecodeError as e:
                    logger.error(f"❌ [{self.account_username}] Sync JSON parsing error: {e}")
                    logger.error(f"❌ [{self.account_username}] Response text (first 500 chars): {response.text[:500] if response.text else 'Empty'}")
                    return {}
                except Exception as e:
                    logger.error(f"❌ [{self.account_username}] Sync parsing error: {e}")
                    import traceback
                    logger.error(f"❌ [{self.account_username}] Traceback: {traceback.format_exc()}")
                    return {}
            else:
                # DEBUG: Log non-200 responses
                logger.error(f"❌ [{self.account_username}] Sync failed with status: {response.status_code}")
                logger.error(f"❌ [{self.account_username}] Response text (first 500 chars): {response.text[:500] if response.text else 'Empty'}")
                
                # FIXED: Handle 401 token expiration properly
                if response.status_code == 401:
                    logger.error(f"🔐 [{self.account_username}] Token is INVALID/EXPIRED (401)")
                    
                    # CRITICAL FIX: Mark token as invalid to trigger refresh
                    old_token_preview = self.matrix_token[:30] if self.matrix_token else "None"
                    self.matrix_token = None
                    self.token_last_refreshed = 0
                    
                    logger.error(f"🔐 [{self.account_username}] Marked token as invalid: {old_token_preview}...")
                    logger.error(f"🔐 [{self.account_username}] Next sync will trigger token refresh")
                    
                elif response.status_code == 403:
                    logger.error(f"🚫 [{self.account_username}] Access FORBIDDEN (403)")
                elif response.status_code == 429:
                    logger.error(f"⏳ [{self.account_username}] RATE LIMITED (429)")
                    # Log rate limit headers if present
                    if 'Retry-After' in response.headers:
                        retry_after = response.headers['Retry-After']
                        logger.error(f"⏳ [{self.account_username}] Retry-After: {retry_after}s")
                elif response.status_code == 502:
                    logger.error(f"🔧 [{self.account_username}] Bad Gateway (502) - server issue")
                elif response.status_code == 503:
                    logger.error(f"🛠️ [{self.account_username}] Service Unavailable (503)")
                
                return {}
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Sync UNHANDLED ERROR: {type(e).__name__}: {e}")
            import traceback
            logger.error(f"❌ [{self.account_username}] Traceback: {traceback.format_exc()}")
            return {}

    def sync(self, timeout=5000):
        """Public sync method with safe wrapper"""
        return self.safe_matrix_request('_sync_raw', timeout=timeout)

    def get_new_messages_from_sync(self):
        """Get ALL new messages directly from sync"""
        try:
            logger.info(f"🔍 [{self.account_username}] DEBUG: Starting get_new_messages_from_sync")
            
            # Check how many processed messages are in the database instead of using an attribute
            try:
                with db_manager.get_cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM processed_messages 
                        WHERE account_username = %s
                    """, (self.account_username,))
                    result = cursor.fetchone()
                    processed_count = result['count'] if result else 0
                    logger.info(f"📊 [{self.account_username}] DEBUG: Processed messages count from DB: {processed_count}")
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Error getting processed message count: {e}")
                processed_count = 0
            
            logger.info(f"📊 [{self.account_username}] DEBUG: Known rooms: {len(self.known_rooms)}")
            logger.info(f"📊 [{self.account_username}] DEBUG: Initial sync pending messages: {len(self.initial_sync_pending_messages)}")
            logger.info(f"📊 [{self.account_username}] DEBUG: next_batch: {self.next_batch}")
            
            if not self.our_matrix_user_id:
                if not self.detect_our_matrix_user_id():
                    logger.warning(f"⚠️ [{self.account_username}] Could not detect our Matrix user ID, skipping message retrieval")
                    return []
            
            if not self.ensure_valid_connection():
                logger.error(f"❌ [{self.account_username}] Cannot get messages: No valid connection")
                return []
            
            # CRITICAL FIX 1: First, return pending messages from initial sync
            if self.initial_sync_pending_messages and not self.initial_sync_processed:
                logger.info(f"📥 [{self.account_username}] Returning {len(self.initial_sync_pending_messages)} pending messages from initial sync")
                pending_messages = self.initial_sync_pending_messages.copy()
                self.initial_sync_pending_messages = []
                self.initial_sync_processed = True
                return pending_messages
            
            # CRITICAL FIX 2: For rooms with existing conversations, force check for pending messages
            sync_data = self.sync(timeout=5000)
            
            if not sync_data:
                logger.info(f"🔍 [{self.account_username}] DEBUG: Sync returned empty data")
                return []
            
            # DEBUG: Log what sync actually returned
            joined_rooms = sync_data.get('rooms', {}).get('join', {})
            invite_rooms = sync_data.get('rooms', {}).get('invite', {})
            
            logger.info(f"🔍 [{self.account_username}] DEBUG: Sync returned {len(joined_rooms)} joined rooms, {len(invite_rooms)} invite rooms")
            
            new_messages = []
            current_time = time.time() * 1000
            
            # FIRST: Process invited rooms (join them)
            for room_id in invite_rooms.keys():
                logger.info(f"🚪 [{self.account_username}] DEBUG: Found new invited room: {room_id[:8]}")
                if room_id not in self.known_rooms:
                    if self.join_room(room_id):
                        self.known_rooms.add(room_id)
                        logger.info(f"✅ [{self.account_username}] Joined invited room {room_id[:8]}")
                    else:
                        logger.warning(f"⚠️ [{self.account_username}] Failed to join invited room {room_id[:8]}")
            
            # SECOND: Process joined rooms with new events
            for room_id, room_data in joined_rooms.items():
                self.known_rooms.add(room_id)
                
                timeline = room_data.get('timeline', {})
                events = timeline.get('events', [])
                
                if not events:
                    continue
                
                # Get room name from state events if available
                room_name = f"Room_{room_id[:8]}"
                state_events = room_data.get('state', {}).get('events', [])
                for event in state_events:
                    if event.get('type') == 'm.room.name':
                        room_name = event.get('content', {}).get('name', room_name)
                
                # Update room metadata
                self.room_metadata.update_room_metadata_from_sync(room_id, events)
                
                # Process message events
                message_events = [e for e in events if e.get('type') == 'm.room.message']
                
                logger.info(f"🔍 [{self.account_username}] DEBUG: Room {room_id[:8]} ({room_name}) has {len(message_events)} message events")
                
                for event in message_events:
                    sender = event.get('sender', '')
                    message_id = event.get('event_id')
                    timestamp = event.get('origin_server_ts', 0)
                    content = event.get('content', {})
                    
                    # Skip our own messages
                    if self.is_our_message(sender):
                        logger.debug(f"🔍 [{self.account_username}] DEBUG: Skipping our own message {message_id[:8]}")
                        continue
                    
                    # Check if already processed
                    if self.persistent_state.is_message_processed(message_id):
                        logger.debug(f"🔍 [{self.account_username}] DEBUG: Message {message_id[:8]} already processed, skipping")
                        continue
                    
                    # Check if should process (timestamp > last reply)
                    if not self.persistent_state.should_process_message(room_id, message_id, timestamp, self.our_matrix_user_id):
                        logger.debug(f"🔍 [{self.account_username}] DEBUG: Message {message_id[:8]} shouldn't process (timestamp <= last reply)")
                        continue
                    
                    # Remove 72-hour age check for existing conversations
                    # Only apply age check if we don't have conversation state
                    # Check if we have conversation state in database
                    has_conversation_state = False
                    try:
                        with db_manager.get_cursor() as cursor:
                            cursor.execute("""
                                SELECT 1 FROM conversation_states 
                                WHERE room_id = %s AND account_username = %s
                                LIMIT 1
                            """, (room_id, self.account_username))
                            has_conversation_state = cursor.fetchone() is not None
                    except Exception as e:
                        logger.error(f"❌ [{self.account_username}] Error checking conversation state: {e}")
                    
                    if not has_conversation_state:
                        # Apply 72-hour age check only for NEW conversations
                        message_age = current_time - timestamp
                        max_age = 72 * 60 * 60 * 1000
                        if message_age > max_age:
                            logger.debug(f"📅 [{self.account_username}] Skipping old message {message_id[:8]} from new conversation (age: {message_age/3600000:.1f}h)")
                            self.persistent_state.mark_message_processed(message_id)
                            continue
                    
                    # DEBUG: Log message details
                    body = content.get('body', '').strip() if content.get('msgtype') != 'm.image' else '[IMAGE]'
                    username = self.extract_reddit_username(sender)
                    logger.info(f"🔍 [{self.account_username}] DEBUG: Found unprocessed message {message_id[:8]} "
                              f"from {username}: {body[:50]}...")
                    
                    username = self.extract_reddit_username(sender)
                    
                    if content.get('msgtype') == 'm.image':
                        mxc_url = content.get('url')
                        if mxc_url:
                            image_description = self.download_and_analyze_image(mxc_url)
                            
                            new_messages.append({
                                'room_id': room_id,
                                'room_name': room_name,
                                'sender': sender,
                                'username': username,
                                'message': f"[IMAGE]: {image_description}",
                                'message_id': message_id,
                                'timestamp': timestamp,
                                'is_image': True,
                                'image_description': image_description
                            })
                    
                    else:
                        body = content.get('body', '').strip()
                        
                        if not body:
                            continue
                        
                        # Check for mod/ban messages
                        if self.is_mod_ban_message(body, sender):
                            logger.info(f"🚫 [{self.account_username}] Filtering out mod/ban message from {username}: {body[:50]}...")
                            self.handle_mod_message(room_id, sender, body)
                            continue
                        
                        new_messages.append({
                            'room_id': room_id,
                            'room_name': room_name,
                            'sender': sender,
                            'username': username,
                            'message': body,
                            'message_id': message_id,
                            'timestamp': timestamp,
                            'is_image': False
                        })
            
            logger.info(f"📥 [{self.account_username}] Found {len(new_messages)} new messages from sync")
            
            # DEBUG: Log summary
            if new_messages:
                logger.info(f"📊 [{self.account_username}] DEBUG: New messages summary:")
                for msg in new_messages:
                    logger.info(f"   📝 Room: {msg['room_id'][:8]}, From: {msg['username']}, "
                              f"Msg: {msg['message'][:50]}..., ID: {msg['message_id'][:8]}")
            
            return new_messages
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Get new messages from sync error: {e}")
            return []

    def _get_joined_rooms_safe(self):
        """Safe wrapper for get_joined_rooms"""
        return self.safe_matrix_request('_get_joined_rooms_raw')
    
    def _get_joined_rooms_raw(self):
        """Raw get_joined_rooms method"""
        try:
            if not self.matrix_token:
                logger.debug(f"🔴 [{self.account_username}] No Matrix token for joined rooms")
                return []
            
            url = f"{self.matrix_base}/joined_rooms"
            
            headers = {
                'Authorization': f'Bearer {self.matrix_token}',
                'Accept': 'application/json'
            }
            
            logger.debug(f"📡 [{self.account_username}] Joined rooms request to: {url}")
            
            start_time = time.time()
            response = self.session.get(url, headers=headers, timeout=30, verify=False)
            elapsed_time = time.time() - start_time
            
            if response:
                logger.debug(f"📊 [{self.account_username}] Joined rooms response status: {response.status_code}, Time: {elapsed_time:.2f}s")
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        room_ids = data.get('joined_rooms', [])
                        logger.debug(f"📊 [{self.account_username}] Found {len(room_ids)} joined rooms: {[room_id[:8] for room_id in room_ids]}")
                        return room_ids
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ [{self.account_username}] Joined rooms JSON decode error: {e}")
                        logger.error(f"❌ [{self.account_username}] Response text: {response.text[:200]}")
                        return []
                else:
                    logger.error(f"❌ [{self.account_username}] Failed to get joined rooms: {response.status_code}")
                    logger.error(f"❌ [{self.account_username}] Response text: {response.text[:200]}")
                    return []
            else:
                logger.error(f"❌ [{self.account_username}] No response for joined rooms")
                return []
                
        except requests.exceptions.Timeout as e:
            logger.error(f"❌ [{self.account_username}] Joined rooms timeout: {e}")
            return []
        except requests.exceptions.ConnectionError as e:
            logger.error(f"❌ [{self.account_username}] Joined rooms connection error: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error getting joined rooms: {e}")
            return []

    def get_joined_rooms(self):
        """Public get_joined_rooms method with safe wrapper"""
        return self._get_joined_rooms_safe()

    def _send_message_safe(self, room_id: str, message: str):
        """Safe wrapper for send_message"""
        return self.safe_matrix_request('_send_message_raw', room_id, message)
    
    def _send_message_raw(self, room_id: str, message: str):
        """Raw send_message method - KEEP FOR API OPERATIONS BUT MARK AS DEPRECATED"""
        try:
            if not self.matrix_token:
                logger.warning(f"⚠️ [{self.account_username}] No Matrix token for API send")
                return False
            
            # This is the OLD API method - keep it but don't use it for chat responses
            # Only use for system operations if needed
            
            logger.warning(f"⚠️ [{self.account_username}] Using API to send message (should use browser instead)")
            
            # ... rest of API code ...
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] API message send error: {e}")
            return False

            


    def _join_room_safe(self, room_id: str):
        """Safe wrapper for join_room"""
        return self.safe_matrix_request('_join_room_raw', room_id)
    
    def _join_room_raw(self, room_id: str):
        """Raw join_room method"""
        try:
            if not self.matrix_token:
                return False
            
            url = f"{self.matrix_base}/join/{room_id}"
            
            headers = {
                'Authorization': f'Bearer {self.matrix_token}',
                'Content-Type': 'application/json'
            }
            
            logger.info(f"🚪 [{self.account_username}] Joining room {room_id[:8]}")
            response = self.session.post(
                url,
                json={},
                headers=headers,
                timeout=30,
                verify=False
            )
            
            if response and response.status_code == 200:
                logger.info(f"✅ [{self.account_username}] Joined room {room_id[:8]}")
                return True
            else:
                logger.error(f"❌ [{self.account_username}] Failed to join room {room_id[:8]}: {response.status_code if response else 'No response'}")
                return False
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Room join error: {e}")
            return False

    def join_room(self, room_id: str):
        """Public join_room method with safe wrapper"""
        return self._join_room_safe(room_id)

    def _get_room_messages_safe(self, room_id: str, limit: int = 100):
        """Safe wrapper for get_room_messages"""
        return self.safe_matrix_request('_get_room_messages_raw', room_id, limit)
    
    def _get_room_messages_raw(self, room_id: str, limit: int = 100):
        """Raw get_room_messages method (only used for history, not new messages)"""
        try:
            if not self.matrix_token:
                return []
            
            url = f"{self.matrix_base}/rooms/{room_id}/messages"
            
            params = {
                'limit': limit,
                'dir': 'b'
            }
            
            headers = {
                'Authorization': f'Bearer {self.matrix_token}'
            }
            
            logger.debug(f"📡 [{self.account_username}] Getting messages for room {room_id[:8]}")
            response = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=30,
                verify=False
            )
            
            if response and response.status_code == 200:
                data = response.json()
                messages = data.get('chunk', [])
                logger.debug(f"📊 [{self.account_username}] Got {len(messages)} messages for room {room_id[:8]}")
                return messages
            else:
                logger.error(f"❌ [{self.account_username}] Failed to get room messages for {room_id[:8]}: {response.status_code if response else 'No response'}")
                return []
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Get messages error for room {room_id[:8]}: {e}")
            return []

    def get_room_messages(self, room_id: str, limit: int = 100):
        """Public get_room_messages method with safe wrapper (for history only)"""
        return self._get_room_messages_safe(room_id, limit)

    def _get_room_members_safe(self, room_id: str):
        """Safe wrapper for get_room_members"""
        return self.safe_matrix_request('_get_room_members_raw', room_id)
    
    def _get_room_members_raw(self, room_id: str):
        """Raw get_room_members method"""
        try:
            if not self.matrix_token:
                return []
            
            url = f"{self.matrix_base}/rooms/{room_id}/members"
            
            headers = {
                'Authorization': f'Bearer {self.matrix_token}'
            }
            
            response = self.session.get(url, headers=headers, timeout=30, verify=False)
            
            if response and response.status_code == 200:
                data = response.json()
                members = data.get('chunk', [])
                return members
            else:
                return []
                
        except Exception as e:
            return []

    def get_room_members(self, room_id: str):
        """Public get_room_members method with safe wrapper"""
        return self._get_room_members_safe(room_id)

    def extract_reddit_username(self, matrix_user_id: str) -> str:
        """Extract Reddit username from Matrix user ID"""
        if matrix_user_id.startswith('@t2_') and ':reddit.com' in matrix_user_id:
            username = matrix_user_id.split(':')[0][1:]
            return username
        return matrix_user_id

    def is_our_message(self, sender: str) -> bool:
        """Check if the message was sent by us"""
        if not self.our_matrix_user_id:
            return False
        
        return sender == self.our_matrix_user_id

    def update_conversation_history(self, room_id: str, room_name: str):
        """Update the conversation history file for a room"""
        try:
            members = self.get_room_members(room_id)
            messages = self.get_room_messages(room_id, limit=50)
            
            if messages is None:
                logger.debug(f"⚠️ [{self.account_username}] Could not get messages for room {room_id[:8]}")
                return False
            
            if members is None:
                members = []
            
            if messages:
                saved_path = self.conversation_manager.save_conversation(room_id, room_name, messages, members, self)
                if saved_path:
                    logger.debug(f"💾 [{self.account_username}] Saved conversation for room {room_id[:8]}")
                return True
            return False
            
        except Exception as e:
            logger.debug(f"⚠️ [{self.account_username}] Error updating conversation history for room {room_id[:8]}: {e}")
            return False

    def get_image_identifier(self, mxc_url: str) -> str:
        """Create a unique identifier for an image from its MXC URL"""
        return hashlib.md5(mxc_url.encode()).hexdigest()

    def download_and_analyze_image(self, mxc_url: str) -> str:
        """Download image from Matrix and analyze it"""
        image_identifier = self.get_image_identifier(mxc_url)
        
        if self.persistent_state.is_image_analyzed(image_identifier):
            cached_analysis = self.persistent_state.get_image_analysis(image_identifier)
            logger.info(f"🖼️ [{self.account_username}] Using cached image analysis for: {image_identifier[:16]}...")
            return cached_analysis
        
        max_retries = 3
        retry_delay = 30
        
        for attempt in range(max_retries):
            try:
                if not self.matrix_token:
                    return "I see an image but can't analyze it right now."
                
                headers = {
                    'Authorization': f'Bearer {self.matrix_token}',
                    'Accept': 'image/*'
                }
                
                image_data = self.direct_downloader.download_image(mxc_url, headers)
                
                if image_data:
                    temp_path = f"temp_image_{int(time.time())}_{self.account_username}.jpg"
                    with open(temp_path, 'wb') as f:
                        f.write(image_data)
                    
                    logger.info(f"🔍 [{self.account_username}] Analyzing downloaded image (direct IP)")
                    description = self.image_analyzer.analyze_image(temp_path)
                    
                    self.persistent_state.mark_image_analyzed(image_identifier, description)
                    
                    try:
                        os.remove(temp_path)
                    except:
                        pass
                    
                    return description
                else:
                    if attempt < max_retries - 1:
                        logger.warning(f"🔄 [{self.account_username}] Image download failed (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay}s...")
                        time.sleep(retry_delay)
                    else:
                        error_msg = "I see you sent an image but couldn't download it for analysis."
                        self.persistent_state.mark_image_analyzed(image_identifier, error_msg)
                        return error_msg
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"🔄 [{self.account_username}] Image download/analysis error (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"❌ [{self.account_username}] Image download/analysis error after {max_retries} attempts: {e}")
                    error_msg = "I see an image but had trouble analyzing it."
                    self.persistent_state.mark_image_analyzed(image_identifier, error_msg)
                    return error_msg
        
        return "I see an image but had trouble analyzing it after multiple attempts."

    def mark_message_as_processed_and_update_cursor(self, room_id: str, message_id: str, timestamp: int):
        """Mark a message as processed and update room cursor - ONLY CALLED AFTER SUCCESSFUL REPLY"""
        try:
            # CRITICAL: Mark message as processed
            self.persistent_state.mark_message_processed(message_id)
            
            # Update room cursor
            self.persistent_state.update_room_cursor(room_id, timestamp)
            
            # Call save_state() - it's now a no-op but exists for compatibility
            self.persistent_state.save_state()
            
            logger.info(f"✅ [{self.account_username}] Marked message {message_id[:8]} as processed AFTER reply "
                      f"for room {room_id[:8]}, timestamp: {timestamp}")
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error in mark_message_as_processed_and_update_cursor: {e}")

    def safe_matrix_request(self, method_name, *args, **kwargs):
        """Make a Matrix API call with automatic error handling - WITH ENHANCED DEBUGGING"""
        self.api_call_count += 1
        
        logger.debug(f"🔍 [{self.account_username}] safe_matrix_request: {method_name} (call #{self.api_call_count})")
        logger.debug(f"🔍 [{self.account_username}] Consecutive failures: {self.consecutive_failures}")
        logger.debug(f"🔍 [{self.account_username}] Circuit breaker tripped: {self.circuit_breaker_tripped}")
        
        if self.circuit_breaker_tripped:
            current_time = time.time()
            if current_time < self.circuit_breaker_reset_time:
                wait_time = self.circuit_breaker_reset_time - current_time
                logger.warning(f"🔴 [{self.account_username}] Circuit breaker active, skipping {method_name} (reset in {wait_time:.1f}s)")
                return None
            else:
                logger.info(f"🟢 [{self.account_username}] Circuit breaker reset")
                self.circuit_breaker_tripped = False
                self.consecutive_failures = 0
        
        if not self.ensure_valid_connection():
            logger.error(f"❌ [{self.account_username}] Cannot make {method_name}: No valid connection")
            return None
        
        method = getattr(self, method_name, None)
        if not method:
            logger.error(f"❌ [{self.account_username}] Method {method_name} not found")
            return None
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                logger.info(f"📡 [{self.account_username}] {method_name} attempt {attempt + 1}/{max_retries}")
                result = method(*args, **kwargs)
                
                if result is not None and result != [] and result != {}:
                    self.consecutive_failures = 0
                    self.circuit_breaker_tripped = False
                    logger.info(f"✅ [{self.account_username}] {method_name} successful")
                    return result
                else:
                    logger.warning(f"⚠️ [{self.account_username}] {method_name} returned empty result, attempt {attempt + 1}/{max_retries}")
                    
                    if attempt < max_retries - 1:
                        backoff_time = 2 ** attempt
                        logger.info(f"⏰ [{self.account_username}] Waiting {backoff_time}s before retry...")
                        time.sleep(backoff_time)
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ [{self.account_username}] {method_name} REQUEST EXCEPTION (attempt {attempt + 1}/{max_retries}): {type(e).__name__}: {e}")
                
                if attempt < max_retries - 1:
                    backoff_time = 5 * (2 ** attempt)
                    logger.info(f"⏰ [{self.account_username}] Waiting {backoff_time}s before retry...")
                    time.sleep(backoff_time)
                
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] {method_name} ERROR (attempt {attempt + 1}/{max_retries}): {type(e).__name__}: {e}")
                import traceback
                logger.error(f"❌ [{self.account_username}] Traceback: {traceback.format_exc()}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        self.consecutive_failures += 1
        logger.warning(f"⚠️ [{self.account_username}] {method_name} failed after {max_retries} attempts")
        
        if self.consecutive_failures >= 3:
            self.circuit_breaker_tripped = True
            self.circuit_breaker_reset_time = time.time() + 10
            logger.error(f"🔴 [{self.account_username}] Circuit breaker tripped after {method_name} failures (reset in 10s)")
        
        return None

    def get_browser_data(self):
        """Get ALL authentication data from browser - MODIFIED TO HANDLE ALL VALID CHAT URLS"""
        try:
            if not self.adspower_browser:
                return None
            
            if not self.adspower_browser.driver:
                if not self.adspower_browser.start_browser():
                    return None
            
            # MODIFIED: Don't navigate to chat.reddit.com if we're already on a valid URL
            current_url = self.adspower_browser.driver.current_url if self.adspower_browser.driver else ""
            
            # Check if we're already on a valid chat interface URL
            valid_patterns = [
                "chat.reddit.com",
                "reddit.com/chat/room/",
                "reddit.com/chat/",  # Add this for old chat interface
                "www.reddit.com/chat/"  # Add this for www version
            ]
            
            is_valid_chat_url = False
            if current_url:
                is_valid_chat_url = any(pattern in current_url for pattern in valid_patterns)
            
            if not is_valid_chat_url:
                # Only navigate if we're not already on a chat URL
                if not self.adspower_browser.navigate_to_chat():
                    return None
            
            browser_data = self.adspower_browser.extract_all_data_from_browser()
            
            if browser_data and browser_data.get('matrix_token'):
                logger.info(f"✅ [{self.account_username}] Extracted all browser data from browser")
                return browser_data
            else:
                logger.error(f"❌ [{self.account_username}] Could not extract browser data from browser")
                return None
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Browser data extraction error: {e}")
            return None
            
# ========== UPDATED IMAGE SENDER CLASS WITH send_image_from_file METHOD ==========
class ImageSender:
    def __init__(self, matrix_token: str, session: requests.Session):
        self.matrix_token = matrix_token
        self.session = session
        self.matrix_base = "https://matrix.redditspace.com/_matrix/client/v3"
        



# ========== MESSAGE GROUPER ==========
class MessageGrouper:
    """Groups messages from the same user in the same room for batch processing"""
    
    def __init__(self, grouping_window=5):  # CHANGED FROM 10 TO 5
        self.grouping_window = grouping_window
        self.pending_messages = {}
        self.last_message_time = {}
    
    def add_message(self, room_id: str, message: dict):
        """Add a message to the grouping queue"""
        current_time = time.time()
        
        if room_id not in self.pending_messages:
            self.pending_messages[room_id] = []
            self.last_message_time[room_id] = current_time
        
        self.pending_messages[room_id].append(message)
        self.last_message_time[room_id] = current_time
        
        logger.debug(f"📨 Added message to group for room {room_id[:8]}. Total in group: {len(self.pending_messages[room_id])}")
    
    def get_ready_messages(self):
        """Get messages that are ready to be processed (grouping window expired)"""
        ready_messages = {}
        current_time = time.time()
        rooms_to_remove = []
        
        for room_id, messages in self.pending_messages.items():
            if not messages:
                continue
                
            time_since_last = current_time - self.last_message_time.get(room_id, 0)
            
            # DEBUG LOGGING
            logger.debug(f"📦 Grouping check for room {room_id[:8]}: "
                        f"{len(messages)} messages, "
                        f"{time_since_last:.1f}s since last (need {self.grouping_window}s)")
            
            if time_since_last >= self.grouping_window and messages:
                ready_messages[room_id] = messages.copy()
                rooms_to_remove.append(room_id)
                logger.info(f"📦 Grouping complete for room {room_id[:8]}. "
                           f"Processing {len(messages)} messages together")
        
        for room_id in rooms_to_remove:
            if room_id in self.pending_messages:
                del self.pending_messages[room_id]
            if room_id in self.last_message_time:
                del self.last_message_time[room_id]
        
        return ready_messages
    
    def add_and_get_ready(self, messages: list):
        """Add messages and immediately get any ready groups (optimized version)"""
        # Add all messages
        for msg in messages:
            self.add_message(msg['room_id'], msg)
        
        # Get ready groups
        return self.get_ready_messages()
        
        # Get ready groups
        return self.get_ready_messages()
    
    def has_pending_messages(self):
        """Check if there are any pending messages waiting to be grouped"""
        return len(self.pending_messages) > 0
    
    def clear_all(self):
        """Clear all pending messages - useful on restart"""
        self.pending_messages = {}
        self.last_message_time = {}
        logger.info("🧹 Cleared all pending messages from grouper")

# ========== UPDATED REDDITCHATBOT CLASS ==========
class RedditChatBot:
    def __init__(self, venice_api_key: str, account_username: str, use_legacy_system: bool = False):
        self.account_username = account_username
        self.use_legacy_system = use_legacy_system  # NEW: Store legacy flag
        debug_trace(f"RedditChatBot.__init__: Created for {account_username} with use_legacy_system={use_legacy_system}")
        
        self.config = ChatBotConfig()
        self.venice_ai = VeniceAI(venice_api_key)
        self.matrix_chat = None
        self.conversation_histories = {}
        self.message_counts = {}
        self.current_stages = {}
        self.image_manager = None
        self.terminated_conversations = set()
        self.message_grouper = MessageGrouper(grouping_window=5)
        
        # Initialize Phase 1 filters
        self.phase1_filter = Phase1Filter(venice_api_key, account_username)
        
        # Initialize Boundary Handler
        self.boundary_handler = BoundaryHandler(venice_api_key, account_username)
        
        # Track user names per room
        self.user_names = {}
        
        # Add random image folder for Phase 2
        self.random_images_sent = {}
        
        self.last_invite_check_time = 0
        self.invite_check_interval = 30
        
        # Add the outgoing check interval (5 minutes = 180 seconds)
        self.outgoing_check_interval = 180
        
        # Concurrent operation flag
        self.is_concurrent = False
        self.response_queue = None
        
        self._load_conversation_states()
    
    def check_bot_status(self):
        """Check bot status and debug"""
        logger.info(f"🔍 [{self.account_username}] Bot status check:")
        logger.info(f"   📊 Conversation histories: {len(self.conversation_histories)} rooms")
        logger.info(f"   📊 Message counts: {len(self.message_counts)} rooms")
        logger.info(f"   📊 Current stages: {len(self.current_stages)} rooms")
        logger.info(f"   🚫 Terminated conversations: {len(self.terminated_conversations)} rooms")
        
        if self.matrix_chat:
            logger.info(f"   📡 Matrix chat initialized: {self.matrix_chat is not None}")
            logger.info(f"   🔑 Matrix token exists: {bool(self.matrix_chat.matrix_token)}")
            # Get processed message count from database instead of attribute
            try:
                with db_manager.get_cursor() as cursor:
                    cursor.execute("""
                        SELECT COUNT(*) as count FROM processed_messages 
                        WHERE account_username = %s
                    """, (self.account_username,))
                    result = cursor.fetchone()
                    processed_count = result['count'] if result else 0
                    logger.info(f"   📁 Processed messages: {processed_count}")
            except Exception as e:
                logger.error(f"   ❌ Error getting processed message count: {e}")
        
        return True
    
    def _load_conversation_states(self):
        """Load conversation states from persistent storage"""
        logger.info(f"📁 [{self.account_username}] Loading conversation states from persistent storage...")
        
        try:
            # Load all rooms with conversation states from database
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT room_id FROM conversation_states 
                    WHERE account_username = %s
                """, (self.account_username,))
                
                results = cursor.fetchall()
                logger.info(f"📁 [{self.account_username}] Found {len(results)} rooms with conversation states in database")
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking conversation states in database: {e}")
    
    def _load_conversation_state_for_room(self, room_id: str):
        """Load conversation state for a specific room from persistent storage"""
        if self.matrix_chat and hasattr(self.matrix_chat, 'persistent_state'):
            state = self.matrix_chat.persistent_state.load_conversation_state(room_id)
            
            if state['history']:
                self.conversation_histories[room_id] = state['history']
                self.message_counts[room_id] = state['message_count']
                self.current_stages[room_id] = state['stage']
                
                logger.info(f"📁 [{self.account_username}] Loaded conversation state for room {room_id[:8]}: "
                          f"{state['message_count']} messages, stage: {state['stage']}")
                return True
        
        return False
    
    def _save_conversation_state_for_room(self, room_id: str):
        """Save conversation state for a room to persistent storage"""
        if (self.matrix_chat and 
            hasattr(self.matrix_chat, 'persistent_state') and 
            room_id in self.conversation_histories):
            
            self.matrix_chat.persistent_state.save_conversation_state(
                room_id,
                self.conversation_histories[room_id],
                self.message_counts.get(room_id, 0),
                self.current_stages.get(room_id, 'intro')
            )
    
    def initialize_matrix_chat_with_retry(self, proxy=None, adspower_profile_id=None, 
                                        account_type='standard', max_retries=3, retry_delay=30):
        """Initialize Matrix chat with retry logic - MODIFIED TO NOT SEND INITIAL OUTGOING"""
        
        for attempt in range(max_retries):
            logger.info(f"🔄 [{self.account_username}] Initialization attempt {attempt + 1}/{max_retries}")
            debug_trace(f"RedditChatBot.initialize_matrix_chat_with_retry: Account {self.account_username}, account_type={account_type}")
            
            self.matrix_chat = RedditMatrixChat(self.account_username, proxy, adspower_profile_id)
            
            if self.matrix_chat.test_connection():
                logger.info(f"✅ [{self.account_username}] Matrix chat initialized successfully on attempt {attempt + 1}")
                
                self.image_manager = self.matrix_chat.image_manager
                
                # Set the appropriate system flag based on account type
                if account_type == 'legacy':
                    self.use_legacy_system = True
                elif account_type == 'both':
                    self.use_legacy_system = True  # For bot tracking, but messenger will handle both
                else:
                    self.use_legacy_system = False
                
                logger.info(f"📤 [{self.account_username}] Account type: {account_type.upper()}")
                
                # Messenger creation is now handled inside RedditMatrixChat based on account_type
                # DO NOT CREATE OUTGOING MESSENGER - OUTGOING MESSAGES DISABLED
                
                self._load_conversation_states_for_all_rooms()
                self.check_initial_pending_conversations()
                
                # DO NOT SEND INITIAL OUTGOING MESSAGE - OUTGOING MESSAGES DISABLED
                logger.info(f"📭 [{self.account_username}] Initial outgoing message NOT sent (outgoing messages disabled)")
                
                return True
            else:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ [{self.account_username}] Initialization failed, retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"❌ [{self.account_username}] Matrix chat initialization failed after {max_retries} attempts")
        
        return False

    def check_initial_pending_conversations(self):
        """Check for pending conversations and invites immediately after initialization"""
        try:
            if not self.matrix_chat:
                return
            
            logger.info(f"🔍 [{self.account_username}] Checking for initial pending conversations...")
            
            # The initial sync already collected pending messages
            pending_count = len(self.matrix_chat.initial_sync_pending_messages)
            if pending_count > 0:
                logger.info(f"📬 [{self.account_username}] Found {pending_count} pending messages from initial sync that need responses")
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error checking initial pending conversations: {e}")
    
    def _load_conversation_states_for_all_rooms(self):
        """Load conversation states for all known rooms"""
        if not self.matrix_chat or not hasattr(self.matrix_chat, 'persistent_state'):
            return
        
        try:
            # Load all rooms with conversation states from database
            with db_manager.get_cursor() as cursor:
                cursor.execute("""
                    SELECT room_id FROM conversation_states 
                    WHERE account_username = %s
                """, (self.account_username,))
                
                results = cursor.fetchall()
                for row in results:
                    room_id = row['room_id']
                    self._load_conversation_state_for_room(room_id)
                
            logger.info(f"📁 [{self.account_username}] Loaded conversation states for {len(results)} rooms from database")
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error loading conversation states from database: {e}")
    
    def detect_stage_from_message(self, message: str, current_stage: str) -> str:
        """Detect if the message should advance the conversation stage"""
        message_lower = message.lower()
        
        stage_hierarchy = {"intro": 1, "flirty": 2, "pitch": 3}
        
        phase3_triggers = [
            "onlyfans", "only fans", "subscription", "price", "cost", "pay", "money",
            "premium", "exclusive", "content", "vip", "paid"
        ]
        
        phase2_triggers = [
            "kinky", "fetish", "slut",
            "fuck", "dick", "cock", "pussy", "ass", "tits", "boobs",
            "jack off", "jerk off", "masturbat", "horny", "turned on",
            "seduce", "flirt", "dirty", "nsfw", "nude", "body",
            "hotter", "sexier", "tits", "bwc", "bbc", "cock", "nudes", "sexy", "hot", "gorgeous", "naked", "looking good", "fuck", "goon", "tits", "titties", "pussy", "vagina", "whore", "mommy", "daddy", "naughty", "holes",
        ]
        
        for trigger in phase3_triggers:
            if trigger in message_lower:
                if current_stage != "pitch":
                    logger.info(f"🔄 [{self.account_username}] Advancing to PHASE 3 (direct inquiry detected)")
                return "pitch"
        
        if current_stage == "intro":
            for trigger in phase2_triggers:
                if trigger in message_lower:
                    logger.info(f"🔄 [{self.account_username}] Advancing to PHASE 2 (flirty message detected)")
                    return "flirty"
        
        room_message_count = self.message_counts.get('current', 0)
        if room_message_count >= 15:  # CHANGED: 25 → 15
            return "pitch"
        elif room_message_count >= 3:  # CHANGED: 11 → 3
            return "flirty"
        else:
            return current_stage
    
    def determine_stage(self, user_message: str, room_id: str, force_phase3: bool = False) -> str:
        """Determine the current conversation stage based on message content and count
        force_phase3: If True, immediately go to Phase 3 (used when out of images)
        """
        # NEW: Check if we're at message limit and haven't pitched
        message_count = self.message_counts.get(room_id, 0)
        has_pitched_of = self._has_pitched_onlyfans(room_id) if hasattr(self, '_has_pitched_onlyfans') else False
        
        if message_count >= 20 and not has_pitched_of:  # CHANGED: 25 → 20
            logger.info(f"🔄 [{self.account_username}] FORCED transition to Phase 3 for room {room_id[:8]} (hit message limit without pitching)")
            self.current_stages[room_id] = "pitch"
            return "pitch"
        
        if force_phase3:
            logger.info(f"🔄 [{self.account_username}] FORCED transition to Phase 3 for room {room_id[:8]} (out of images)")
            self.current_stages[room_id] = "pitch"
            return "pitch"
        
        current_stage = self.current_stages.get(room_id, "intro")
        message_count = self.message_counts.get(room_id, 0)
        
        new_stage = self.detect_stage_from_message(user_message, current_stage)
        
        stage_hierarchy = {"intro": 1, "flirty": 2, "pitch": 3}
        current_level = stage_hierarchy.get(current_stage, 1)
        new_level = stage_hierarchy.get(new_stage, 1)
        
        if new_level > current_level:
            self.current_stages[room_id] = new_stage
            return new_stage
        
        if message_count >= 15:  # CHANGED: 25 → 15
            self.current_stages[room_id] = "pitch"
        elif message_count >= 3 and current_stage == "intro":  # CHANGED: 11 → 3
            self.current_stages[room_id] = "flirty"
        
        return self.current_stages.get(room_id, "intro")
    
    def should_send_image(self, user_message: str, current_stage: str, room_id: str) -> tuple:
        """Determine if we should send an image and what type
        Returns: (should_send, image_type, trigger_phase3_if_no_images, out_of_images_for_type)
        """
        if current_stage == "intro":
            return False, None, False, False
        
        image_type = self.image_manager.detect_image_request(user_message)
        if image_type:
            has_unsent = self.image_manager.has_unsent_images_for_type(image_type, room_id)
            if has_unsent:
                logger.info(f"📸 [{self.account_username}] Image trigger detected: {image_type} for room {room_id[:8]}")
                return True, image_type, False, False
            else:
                logger.info(f"📸 [{self.account_username}] Image trigger detected but NO UNSENT {image_type} images available for room {room_id[:8]}")
                # Return that we're out of images for this type
                return False, image_type, True, True
        
        return False, None, False, False
    
    def should_continue_conversation(self, room_id: str, user_message: str, user_id: str) -> bool:
        """Determine if we should continue this conversation or end it"""
        # FIRST CHECK: If conversation already terminated, NEVER respond again
        if room_id in self.terminated_conversations:
            logger.info(f"⏹️  [{self.account_username}] Conversation with room {room_id[:8]} already terminated, ignoring all future messages")
            return False
        
        # Check for mod/ban messages
        if self.matrix_chat and self.matrix_chat.is_mod_ban_message(user_message, user_id):
            logger.info(f"🚫 [{self.account_username}] Mod/ban message detected, ending conversation permanently")
            self.matrix_chat.conversation_manager.mark_conversation_completed(user_id)
            self.terminated_conversations.add(room_id)
            return False
        
        # Check if conversation already completed globally
        if self.matrix_chat.conversation_manager.is_conversation_completed(user_id):
            logger.info(f"⏹️  [{self.account_username}] Conversation with user {user_id} already completed globally, ignoring")
            self.terminated_conversations.add(room_id)
            return False
        
        # Check local message count limit
        message_count = self.message_counts.get(room_id, 0)
        current_stage = self.current_stages.get(room_id, "intro")
        
        # Check if we've sent the OnlyFans link
        has_sent_link = self._has_pitched_onlyfans(room_id)
        
        # Message limit reached - but check if we've sent the link
        if message_count >= 20:  # CHANGED: 25 → 20
            if not has_sent_link:
                logger.info(f"⚠️ [{self.account_username}] Hit 20 messages but haven't sent OnlyFans link yet - FORCING link send")
                # Force transition to Phase 3 for the next response
                self.current_stages[room_id] = "pitch"
                return True  # Continue to send the link
            
            logger.info(f"⏹️  [{self.account_username}] Conversation limit reached (20 messages) and OnlyFans link sent for room {room_id[:8]}, terminating permanently")
            self.matrix_chat.conversation_manager.mark_conversation_completed(user_id)
            self.terminated_conversations.add(room_id)
            return False
        
        # Phase 3 specific termination logic (ONLY applies if we're already in Phase 3)
        if current_stage == "pitch" and message_count >= 18:  # CHANGED: 22 → 18
            buying_intent_triggers = [
                "how much", "price", "cost", "subscribe", "sign up", "payment",
                "credit card", "paypal", "onlyfans", "only fans", "link", "username"
            ]
            
            message_lower = user_message.lower()
            has_buying_intent = any(trigger in message_lower for trigger in buying_intent_triggers)
            
            # Count messages sent in Phase 3
            phase3_message_count = 0
            if room_id in self.conversation_histories:
                for msg in self.conversation_histories[room_id]:
                    if msg.get("sender") == "bot" and self.current_stages.get(room_id) == "pitch":
                        phase3_message_count += 1
            
            # Terminate after 2+ messages in Phase 3 without buying intent
            # BUT only if we've already sent the link!
            if not has_buying_intent and phase3_message_count >= 2 and has_sent_link:
                logger.info(f"⏹️  [{self.account_username}] Terminating after {phase3_message_count} messages in Phase 3 without buying intent (link already sent)")
                self.matrix_chat.conversation_manager.mark_conversation_completed(user_id)
                self.terminated_conversations.add(room_id)
                return False
        
        return True
    
    def _has_pitched_onlyfans(self, room_id: str) -> bool:
        """Check if we've mentioned OnlyFans link in this conversation - ONLY checks for actual links"""
        if room_id not in self.conversation_histories:
            return False
        
        # ONLY check for actual links - not keywords
        link_variations = [
            "onlyfans.com/rileyboo.tv",  # Standard link
            "onlyfans .com/rileyboo.tv", # With space (for security)
            "rileyboo.tv"                # Just domain
        ]
        
        # Check bot messages for OnlyFans links
        for msg in self.conversation_histories[room_id]:
            if msg.get("sender") == "bot":
                message_text = msg.get("text", "").lower()
                
                # Remove spaces from message for easier matching
                message_no_spaces = message_text.replace(" ", "")
                
                # Check each link variation
                for link in link_variations:
                    link_lower = link.lower()
                    
                    # Check standard version
                    if link_lower in message_text:
                        logger.debug(f"✅ [{self.account_username}] Found OnlyFans link in conversation: {link_lower}")
                        return True
                    
                    # Also check without spaces (for "onlyfans .com" detection)
                    link_no_spaces = link_lower.replace(" ", "")
                    if link_no_spaces in message_no_spaces:
                        logger.debug(f"✅ [{self.account_username}] Found OnlyFans link (no spaces) in conversation: {link_no_spaces}")
                        return True
        
        logger.debug(f"📭 [{self.account_username}] No OnlyFans link found in conversation history for room {room_id[:8]}")
        return False
    
    
    def generate_termination_message(self, room_id: str) -> str:
        """Generate a final message to end the conversation"""
        # NEW: Check if we need to pitch OnlyFans before terminating
        if not self._has_pitched_onlyfans(room_id):
            logger.info(f"🎯 [{self.account_username}] Haven't pitched OnlyFans yet, sending pitch before termination")
            # Force a pitch message
            return self._generate_onlyfans_pitch_message(room_id)
        
        current_stage = self.current_stages.get(room_id, "intro")
        
        if current_stage == "pitch":
            return "It was fun chatting! If you change your mind about my OnlyFans, you know where to find me 😘 Take care!"
        else:
            return "Thanks for the chat! I've got to run now. Was nice talking to you! 👋"
            
    def _generate_onlyfans_pitch_message(self, room_id: str) -> str:
        """Generate an OnlyFans pitch message with the link"""
        # Try to generate a natural pitch using Venice AI
        try:
            # Get conversation context
            conversation_context = ""
            if room_id in self.conversation_histories and self.conversation_histories[room_id]:
                # Get last few messages for context
                recent_messages = self.conversation_histories[room_id][-3:]
                for msg in recent_messages:
                    sender = "Them" if msg.get("sender") == "user" else "Me"
                    conversation_context += f"{sender}: {msg.get('text', '')}\n"
            
            prompt = f'''Based on this conversation, create a natural, flirty pitch for my OnlyFans.
            Include the link with a space for security: onlyfans .com/rileyboo.tv
            Make it sound like a natural transition from our chat.
            Keep it 2-3 sentences max with 1-2 emojis.
            
            Conversation:
            {conversation_context}
            
            Pitch message:'''
            
            response = self.venice_ai.generate_response(
                [],  # Empty conversation history for this specific prompt
                self.config,
                "pitch"
            )
            
            # Ensure the link is included (with space for security)
            if "onlyfans.com/rileyboo.tv" not in response.lower() and \
               "onlyfans .com/rileyboo.tv" not in response.lower() and \
               "rileyboo.tv" not in response.lower():
                response = f"{response}\n\nIf you want to see way more of me, check out my OnlyFans: onlyfans .com/rileyboo.tv 😈"
            
            return response
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error generating OnlyFans pitch: {e}")
            # Fallback pitch with spaced link
            fallback_pitches = [
                "Hey, I've really enjoyed chatting with you! If you want to see way more of me, check out my OnlyFans: onlyfans .com/rileyboo.tv 😈",
                "This was fun! I have way hotter content on my OnlyFans if you want to see more: onlyfans .com/rileyboo.tv 😘",
                "I've got to run, but if you enjoyed this, you'd love my OnlyFans: onlyfans .com/rileyboo.tv - way more content there! 🔥"
            ]
            return random.choice(fallback_pitches)

    def setup_concurrent_operation(self, response_queue):
        """Setup bot for concurrent operation"""
        self.response_queue = response_queue
        self.is_concurrent = True
        logger.info(f"⚡ [{self.account_username}] Setup for concurrent operation")

    def generate_response_for_grouped_messages(self, room_id: str, messages: list, username: str, user_id: str):
        """Generate a single response for multiple grouped messages with Phase 1 filters - WITH DEBUG"""
        # CRITICAL: Check if messages list is empty
        if not messages:
            logger.warning(f"⚠️ [{self.account_username}] Empty messages list for room {room_id[:8]}, skipping")
            return "NO_MESSAGES_TO_PROCESS", None, False, None
        
        logger.info(f"🔧 [{self.account_username}] Generating response for {len(messages)} messages from {username}")
        
        # DEBUG: Log all incoming messages
        logger.info(f"[DEBUG GROUPED MESSAGES] Room: {room_id[:8]}, User: {username}")
        for i, msg in enumerate(messages, 1):
            logger.info(f"[DEBUG MESSAGE {i}/{len(messages)}]: {msg['message'][:100]}...")
            if msg.get('is_image'):
                logger.info(f"[DEBUG IMAGE DESCRIPTION {i}]: {msg.get('image_description', 'No description')[:100]}...")
        
        # CRITICAL FIX: Check if conversation already terminated BEFORE processing
        if room_id in self.terminated_conversations:
            logger.info(f"⏹️  [{self.account_username}] Room {room_id[:8]} already terminated, ignoring {len(messages)} new messages")
            return "CONVERSATION_ALREADY_TERMINATED"
        
        if room_id not in self.conversation_histories:
            loaded = self._load_conversation_state_for_room(room_id)
            if not loaded:
                self.conversation_histories[room_id] = []
                self.message_counts[room_id] = 0
                self.current_stages[room_id] = "intro"
        
        last_message = messages[-1]['message']
        
        # Check each message for mod/ban content
        for msg in messages:
            if self.matrix_chat and self.matrix_chat.is_mod_ban_message(msg['message'], msg.get('sender')):
                logger.info(f"🚫 [{self.account_username}] Found mod/ban message in group, ending conversation permanently")
                self.matrix_chat.conversation_manager.mark_conversation_completed(user_id)
                self.terminated_conversations.add(room_id)  # Mark as terminated
                return "CONVERSATION_TERMINATED_MOD_MESSAGE"
        
        # Check if we should continue BEFORE processing messages
        if not self.should_continue_conversation(room_id, last_message, user_id):
            # If should_continue_conversation returned False, it already marked conversation as terminated
            # Just return the termination message
            termination_msg = self.generate_termination_message(room_id)
            return termination_msg, None, False, None
        
        # Only process messages if conversation is still active
        image_descriptions = []
        has_image = False
        force_phase3_due_to_no_images = False
        requested_image_type_with_no_images = None
        
        for msg in messages:
            # Handle objections
            objection_handling = self.handle_objection(msg['message'], room_id)
            if objection_handling:
                logger.info(f"🛡️  [{self.account_username}] Objection detected, using objection response")
                return objection_handling, None, True, None
                
            # NEW: Check for boundary requests (social media/preview requests)
            request_type, triggers = self.boundary_handler.detect_boundary_request(msg['message'])
            if request_type:
                logger.info(f"🛡️  [{self.account_username}] Boundary request detected: {request_type} (triggers: {triggers})")
                
                # Build conversation context for Venice AI
                conversation_context = ""
                if self.conversation_histories.get(room_id):
                    recent_msgs = self.conversation_histories[room_id][-3:] if len(self.conversation_histories[room_id]) >= 3 else self.conversation_histories[room_id]
                    for history_msg in recent_msgs:
                        sender = "Them" if history_msg.get("sender") == "user" else "You"
                        conversation_context += f"{sender}: {history_msg.get('text', '')}\n"
                
                # Check if we're out of images for this type of request
                out_of_images = False
                if request_type == "preview":
                    # Check if image manager has images left
                    if hasattr(self, 'image_manager') and self.image_manager:
                        # Check for common image types they might be asking for
                        image_type = self.image_manager.detect_image_request(msg['message'])
                        if image_type:
                            out_of_images = not self.image_manager.has_unsent_images_for_type(image_type, room_id)
                
                # Generate boundary response using Venice AI
                boundary_response = self.boundary_handler.generate_boundary_response(
                    msg['message'], 
                    request_type, 
                    triggers,
                    self.current_stages.get(room_id, "intro"),
                    conversation_context,
                    self.config,
                    out_of_images
                )
                
                if boundary_response:
                    logger.info(f"🛡️  [{self.account_username}] Using boundary response for {request_type} request")
                    
                    # Add boundary response to conversation history
                    self.conversation_histories[room_id].append({
                        "sender": "bot", 
                        "text": boundary_response,
                        "timestamp": datetime.now().isoformat(),
                        "is_boundary_response": True
                    })
                    
                    # Return boundary response (no image, continue conversation)
                    return boundary_response, None, True, None
            
            # Add to conversation history
            self.conversation_histories[room_id].append({
                "sender": "user",
                "text": msg['message'],
                "username": username,
                "user_id": user_id,
                "timestamp": datetime.now().isoformat(),
                "is_image": msg.get('is_image', False),
                "image_description": msg.get('image_description'),
                "has_objection": bool(objection_handling)
            })
            
            if msg.get('is_image', False) and msg.get('image_description'):
                has_image = True
                image_descriptions.append(msg['image_description'])
            
            previous_stage = self.current_stages[room_id]
            
            # FIXED: Changed from 3 to 4 values unpacking
            should_send, image_type, trigger_phase3_if_no_images, out_of_images_for_type = self.should_send_image(msg['message'], self.current_stages[room_id], room_id)
            if trigger_phase3_if_no_images:
                force_phase3_due_to_no_images = True
                requested_image_type_with_no_images = image_type
                logger.info(f"🚨 [{self.account_username}] User requested {image_type} but we have no unsent images - FORCING Phase 3 transition")
            
            # Track if we're out of images for boundary handler
            if out_of_images_for_type:
                logger.debug(f"📭 [{self.account_username}] Marked as out of images for {image_type} in room {room_id[:8]}")
                
            if force_phase3_due_to_no_images:
                self.current_stages[room_id] = self.determine_stage(msg['message'], room_id, force_phase3=True)
            else:
                self.current_stages[room_id] = self.determine_stage(msg['message'], room_id)
            
            if self.current_stages[room_id] != previous_stage:
                logger.info(f"🔄 [{self.account_username}] Room {room_id[:8]} stage changed: {previous_stage.upper()} → {self.current_stages[room_id].upper()}")
        
        # FIXED: Changed from 3 to 4 values unpacking
        should_send, image_type, trigger_phase3_if_no_images, out_of_images_for_type = self.should_send_image(last_message, self.current_stages[room_id], room_id)
        image_path_or_url = None
        is_cached_url = False
        
        if should_send and image_type and not trigger_phase3_if_no_images:
            cached_url, file_path = self.image_manager.get_random_image(image_type, room_id)
            
            if cached_url:
                image_path_or_url = cached_url
                is_cached_url = True
                logger.info(f"📸 [{self.account_username}] Will send cached {image_type} image URL to room {room_id[:8]}")
            elif file_path:
                image_path_or_url = file_path
                is_cached_url = False
                logger.info(f"📸 [{self.account_username}] Will upload and send {image_type} image: {os.path.basename(file_path)} to room {room_id[:8]}")
            else:
                logger.info(f"📸 [{self.account_username}] No unsent {image_type} images available for room {room_id[:8]}")
                requested_image_type_with_no_images = image_type
                force_phase3_due_to_no_images = True
        
        # Extract user name from messages if present
        if room_id not in self.user_names:
            for msg in messages:
                extracted_name = self.phase1_filter.extract_user_name(msg['message'])
                if extracted_name:
                    self.user_names[room_id] = extracted_name
                    self.phase1_filter.set_user_name(extracted_name)
                    logger.info(f"📝 [{self.account_username}] Extracted user name for room {room_id[:8]}: {extracted_name}")
                    break
        
        # Combine image descriptions
        combined_image_description = None
        if image_descriptions:
            if len(image_descriptions) == 1:
                combined_image_description = image_descriptions[0]
            else:
                combined_image_description = f"The user sent {len(image_descriptions)} images. " + " ".join(image_descriptions)
        
        # Add Phase 3 transition context if needed
        phase3_prompt_extra = ""
        if force_phase3_due_to_no_images and requested_image_type_with_no_images:
            body_part_map = {
                'ass': 'ass',
                'tits': 'boobs/tits',
                'feet': 'feet',
                'selfie': 'chongalong221',
                'random': 'more'
            }
            body_part = body_part_map.get(requested_image_type_with_no_images, requested_image_type_with_no_images)
            
            phase3_prompt_extra = f"""
SPECIAL CONTEXT: The user just requested to see more of your {body_part}, but you've already shown them all the {body_part} pictures you share for free on Reddit. 
You should transition the conversation to your OnlyFans by mentioning that you have way more {body_part} content there including longer videos.
Say something like "I have way more {body_part} pics and even long videos I can show you on my OnlyFans, I know you'd love them!"
Be natural and flirty about it, don't make it sound like a hard sell."""
        
        # Generate response using Venice AI
        try:
            logger.info(f"[DEBUG CALLING VENICE AI] Stage: {self.current_stages[room_id]}, Has image: {has_image}")
            if has_image:
                logger.info(f"[DEBUG IMAGE DESCRIPTIONS]: {combined_image_description[:200]}...")
            if phase3_prompt_extra:
                logger.info(f"[DEBUG PHASE3 EXTRA CONTEXT]: {phase3_prompt_extra[:200]}...")
            
            bot_response = self.venice_ai.generate_response_with_context(
                self.conversation_histories[room_id], 
                self.config, 
                self.current_stages[room_id],
                combined_image_description if has_image else None,
                phase3_prompt_extra
            )
            
            logger.info(f"🤖 [{self.account_username}] Venice AI generated response: {bot_response[:100]}...")
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Venice AI error: {e}")
            # Fallback to a simple response
            current_stage = self.current_stages.get(room_id, "intro")
            if current_stage == "pitch":
                bot_response = "I've got so much more content on my OnlyFans you'd love! Want to check it out? 😈"
            elif current_stage == "flirty":
                bot_response = "You're making me so horny right now... 😉"
            else:
                bot_response = "Hey there! Thanks for messaging me! 😊"
        
        # Apply Phase 1 filters if in intro stage
        current_stage = self.current_stages.get(room_id, "intro")
        if current_stage == "intro":
            try:
                # Filter DM questions
                filtered_response = self.phase1_filter.filter_dm_questions(bot_response)
                
                # Filter user name if we have it
                if room_id in self.user_names:
                    self.phase1_filter.set_user_name(self.user_names[room_id])
                    filtered_response = self.phase1_filter.filter_user_name(filtered_response)
                
                if filtered_response != bot_response:
                    logger.info(f"🛡️  [{self.account_username}] Applied Phase 1 filters to response")
                
                bot_response = filtered_response
            except Exception as e:
                logger.error(f"❌ [{self.account_username}] Phase 1 filter error: {e}")
        
        # Add to conversation history
        self.conversation_histories[room_id].append({
            "sender": "bot", 
            "text": bot_response,
            "timestamp": datetime.now().isoformat()
        })
        
        # Update message count
        self.message_counts[room_id] = len([msg for msg in self.conversation_histories[room_id] if msg["sender"] == "user"])
        
        # Get random image for Phase 2 if applicable
        random_image_info = None
        random_image_is_cached = False
        if current_stage == "flirty" and not force_phase3_due_to_no_images:
            cached_random_url, random_file_path = self.image_manager.get_random_random_image(room_id)
            if cached_random_url or random_file_path:
                # FIXED: Handle None values properly
                if cached_random_url:
                    random_image_info = cached_random_url
                    random_image_is_cached = True
                else:
                    random_image_info = random_file_path
                    random_image_is_cached = False
                logger.info(f"🎲 [{self.account_username}] Will send random image in Phase 2 for room {room_id[:8]}")
        
        # Save conversation state
        self._save_conversation_state_for_room(room_id)
        
        logger.info(f"✅ [{self.account_username}] Successfully generated response for {username}")
        
        # FIXED: Return tuple with ALL 4 values, ensuring random_image_info is a tuple
        return bot_response, (image_path_or_url, is_cached_url) if image_path_or_url else (None, False), True, (random_image_info, random_image_is_cached) if random_image_info else (None, False)
        
    # ========== END CONCURRENT METHODS ==========
    
    def handle_objection(self, user_message: str, room_id: str) -> str:
        """Handle user objections with predefined responses"""
        objection_response = self.config.get_objection_response(user_message)
        if objection_response:
            logger.info(f"🛡️  [{self.account_username}] Handling objection for room {room_id[:8]}: {user_message[:50]}...")
            return objection_response
        
        return None
    
    
    def check_invites_cycle(self):
        """Check for new invites - now handled automatically by sync"""
        try:
            current_time = time.time()
            if current_time - self.last_invite_check_time >= self.invite_check_interval:
                logger.info(f"🚪 [{self.account_username}] Sync automatically handles invites, no need for separate check")
                self.last_invite_check_time = current_time
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error in check invites cycle: {e}")
            return False
    
    def _save_all_conversation_states(self):
        """Save all conversation states before exiting"""
        if not self.matrix_chat or not hasattr(self.matrix_chat, 'persistent_state'):
            return
        
        for room_id in self.conversation_histories.keys():
            self._save_conversation_state_for_room(room_id)
        
        logger.info(f"💾 [{self.account_username}] Saved all conversation states before exit")

class AccountManager:
    """Manages multiple account configurations"""
    
    def __init__(self):
        self.accounts = {}
        self.load_accounts()
    
    def load_accounts(self):
        """Load accounts from accounts.txt, accounts2.txt, and accounts3.txt"""
        adspower_accounts = {}
        account_types = {}  # Track which accounts use which system: 'standard', 'legacy', or 'both'
        
        try:
            # Load accounts3.txt FIRST (BOTH systems)
            if os.path.exists('accounts3.txt'):
                with open('accounts3.txt', 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and ':' in line:
                            username, profile_id = line.split(':', 1)
                            username = username.strip()
                            profile_id = profile_id.strip()
                            adspower_accounts[username] = profile_id
                            account_types[username] = 'both'  # BOTH systems
                            debug_trace(f"AccountManager: Loaded {username} from accounts3.txt (type=both)")
                logger.info(f"👥 Loaded {len([u for u in account_types if account_types[u] == 'both'])} dual-system accounts from accounts3.txt")
            
            # Load high CQS accounts (standard method) from accounts.txt
            if os.path.exists('accounts.txt'):
                with open('accounts.txt', 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and ':' in line:
                            username, profile_id = line.split(':', 1)
                            username = username.strip()
                            profile_id = profile_id.strip()
                            # Don't overwrite accounts3.txt entries
                            if username not in adspower_accounts:
                                adspower_accounts[username] = profile_id
                                account_types[username] = 'standard'  # Standard method
                                debug_trace(f"AccountManager: Loaded {username} from accounts.txt (type=standard)")
                logger.info(f"👥 Loaded {len([u for u in account_types if account_types[u] == 'standard'])} standard accounts from accounts.txt")
            
            # Load low CQS accounts (legacy method) from accounts2.txt
            if os.path.exists('accounts2.txt'):
                with open('accounts2.txt', 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and ':' in line:
                            username, profile_id = line.split(':', 1)
                            username = username.strip()
                            profile_id = profile_id.strip()
                            # Don't overwrite accounts3.txt entries
                            if username not in adspower_accounts:
                                adspower_accounts[username] = profile_id
                                account_types[username] = 'legacy'  # Legacy method
                                debug_trace(f"AccountManager: Loaded {username} from accounts2.txt (type=legacy)")
                logger.info(f"👥 Loaded {len([u for u in account_types if account_types[u] == 'legacy'])} legacy accounts from accounts2.txt")
                
        except Exception as e:
            logger.warning(f"⚠️ Error loading accounts files: {e}")
        
        proxies = []
        try:
            with open('proxies.txt', 'r') as f:
                proxies = [line.strip() for line in f if line.strip()]
            logger.info(f"🔌 Loaded {len(proxies)} proxies")
        except FileNotFoundError:
            logger.warning("⚠️ No proxies.txt file found, running without proxy")
        
        for username, profile_id in adspower_accounts.items():
            proxy = proxies[len(self.accounts) % len(proxies)] if proxies else None
            account_type = account_types.get(username, 'standard')
            
            self.accounts[username] = {
                'proxy': proxy,
                'adspower_profile_id': profile_id,
                'account_type': account_type  # NEW: Store account type
            }
            
            method = account_type.upper()
            logger.info(f"👤 Account configured: {username} ({method} method)")
            debug_trace(f"AccountManager: Configured {username} with account_type={account_type}")
            
            if proxy:
                logger.info(f"   🔌 Proxy: {proxy[:50]}...")
            if profile_id:
                logger.info(f"   🖥️  AdsPower profile ID: {profile_id}")
    
    def get_accounts(self):
        """Get all account configurations"""
        return self.accounts

# ========== CONCURRENT BOT MANAGER ==========
class ConcurrentBotManager:
    """Manages concurrent execution of multiple bots with independent threads"""
    
    def __init__(self):
        """Initialize ConcurrentBotManager"""
        self.bots = {}
        self.is_running = False
        self.threads = {}
        self.response_queues = {}
        self.message_groupers = {}  # ADD THIS LINE
        logger.info("🚀 Concurrent Bot Manager initialized")
        
    def add_bot(self, username: str, bot: RedditChatBot):
        """Add a bot to the manager with its own thread pool"""
        self.bots[username] = bot
        self.response_queues[username] = queue.Queue()
        self.message_groupers[username] = MessageGrouper(grouping_window=5)  # ADD THIS
        
        # Initialize bot for concurrent operation
        bot.setup_concurrent_operation(self.response_queues[username])
        
        logger.info(f"📝 [{username}] Added to concurrent bot manager")
    
    def start_all_bots(self):
        """Start all bots concurrently"""
        self.is_running = True
        logger.info(f"🚀 Starting {len(self.bots)} bots concurrently...")
        
        for username, bot in self.bots.items():
            # Start 4 independent threads for each bot
            self._start_bot_threads(username, bot)
        
        logger.info(f"✅ All {len(self.bots)} bots running concurrently")
    
    def _start_bot_threads(self, username: str, bot: RedditChatBot):
        """Start 6 independent threads for a single bot"""
        logger.info(f"🧵 [{username}] Starting 6 concurrent threads...")
        
        # Thread 1: Continuous Message Checker
        message_thread = threading.Thread(
            target=self._bot_message_checker_loop,
            args=(username, bot),
            name=f"{username}_message_checker",
            daemon=True
        )
        
        # Thread 2: Continuous Response Sender
        response_thread = threading.Thread(
            target=self._bot_response_sender_loop,
            args=(username, bot),
            name=f"{username}_response_sender",
            daemon=True
        )
        
        # Thread 3: Outgoing Message Sender (handles both systems if applicable)
        outgoing_thread = threading.Thread(
            target=self._bot_outgoing_sender_loop,
            args=(username, bot),
            name=f"{username}_outgoing_sender",
            daemon=True
        )
        
        # Thread 4: Periodic Invite Checker
        invite_thread = threading.Thread(
            target=self._bot_invite_checker_loop,
            args=(username, bot),
            name=f"{username}_invite_checker",
            daemon=True
        )
        
        # Thread 5: Legacy Room Checker
        legacy_room_thread = threading.Thread(
            target=self._bot_legacy_room_checker_loop,
            args=(username, bot),
            name=f"{username}_legacy_room_checker",
            daemon=True
        )
        
        # Thread 6: Dual System Checker (for accounts3.txt)
        dual_system_thread = threading.Thread(
            target=self._bot_dual_system_checker_loop,
            args=(username, bot),
            name=f"{username}_dual_system_checker",
            daemon=True
        )
        
        # Store threads
        self.threads[username] = {
            'message': message_thread,
            'response': response_thread,
            'outgoing': outgoing_thread,
            'invite': invite_thread,
            'legacy_room': legacy_room_thread,
            'dual_system': dual_system_thread
        }
        
        # Start all threads
        message_thread.start()
        response_thread.start()
        outgoing_thread.start()
        invite_thread.start()
        legacy_room_thread.start()
        dual_system_thread.start()
        
        # Log what each thread does
        logger.info(f"🧵 [{username}] Threads started:")
        logger.info(f"   🔍 Message Checker: Continuous message checking (every 1s)")
        logger.info(f"   💬 Response Sender: Processes response queue")
        logger.info(f"   📤 Outgoing Sender: Sends messages (immediately, then every 5m)")
        logger.info(f"   🚪 Invite Checker: Checks for invites (every 30s)")
        logger.info(f"   🔍 Legacy Room Checker: Checks for new rooms from legacy messages (every 30s)")
        logger.info(f"   🔄 Dual System Checker: Ensures both systems are running (every 60s)")
        
    def _bot_dual_system_checker_loop(self, username: str, bot: RedditChatBot):
        """Dual system checker loop - MODIFIED TO DO NOTHING"""
        logger.info(f"🔄 [{username}] Dual system checker thread DISABLED")
        
        while self.is_running:
            try:
                # DO NOTHING - Outgoing messages are disabled
                logger.debug(f"📭 [{username}] Outgoing messages disabled")
                time.sleep(60)
                
            except Exception as e:
                logger.error(f"❌ [{username}] Dual system checker error: {e}")
                time.sleep(60)
        
        logger.info(f"🛑 [{username}] Dual system checker thread stopped")
        
    def is_bot_ready_for_outgoing(self, username: str) -> bool:
        """Check if a bot is ready to send outgoing messages"""
        if username not in self.bots:
            return False
        
        bot = self.bots[username]
        
        # Check if matrix_chat is initialized
        if not hasattr(bot, 'matrix_chat') or not bot.matrix_chat:
            logger.debug(f"📭 [{username}] Not ready: matrix_chat not initialized")
            return False
        
        # Check if outgoing_messenger exists
        if not hasattr(bot.matrix_chat, 'outgoing_messenger') or not bot.matrix_chat.outgoing_messenger:
            logger.debug(f"📭 [{username}] Not ready: outgoing_messenger not created")
            return False
        
        # Check if we have a valid connection
        if not hasattr(bot.matrix_chat, 'matrix_token') or not bot.matrix_chat.matrix_token:
            logger.debug(f"📭 [{username}] Not ready: no matrix token")
            return False
        
        return True

    # Add this method to ConcurrentBotManager class:
    def cleanup_all(self):
        """Clean up all bot resources"""
        logger.info("🧹 Cleaning up all bot resources...")
        for username, bot in self.bots.items():
            try:
                if hasattr(bot, 'matrix_chat') and bot.matrix_chat:
                    bot.matrix_chat.cleanup()
                    logger.debug(f"🧹 Cleaned up resources for {username}")
            except Exception as e:
                logger.debug(f"⚠️ Error cleaning up {username}: {e}")
        
        logger.info("✅ All resources cleaned up")

    def _bot_message_checker_loop(self, username: str, bot: RedditChatBot):
        """Continuous message checking loop (runs every 1 second)"""
        logger.info(f"🔍 [{username}] Message checker thread started")  # FIXED: Use username parameter
        
        # Get the persistent message grouper
        message_grouper = self.message_groupers[username]
        
        # Track messages currently being processed to avoid duplicates
        messages_in_progress = set()
        
        # Track the last time we actually found new messages
        last_actual_new_messages = 0
        message_check_cooldown = 2  # Don't check for new messages more often than every 2 seconds
        
        while self.is_running:
            try:
                current_time = time.time()
                
                # Only check for new messages if enough time has passed since last check
                if current_time - last_actual_new_messages < message_check_cooldown:
                    # Wait a bit before checking again
                    time.sleep(0.5)
                    continue
                
                # Get new messages
                new_messages = bot.matrix_chat.get_new_messages_from_sync()
                
                if new_messages:
                    logger.info(f"📥 [{username}] Found {len(new_messages)} ACTUAL new message(s)")
                    last_actual_new_messages = current_time
                    
                    # Filter out messages already in progress
                    filtered_messages = []
                    for msg in new_messages:
                        msg_key = f"{msg['room_id']}_{msg['message_id']}"
                        if msg_key in messages_in_progress:
                            logger.debug(f"🔄 [{username}] Message {msg['message_id'][:8]} already in progress, skipping")
                            continue
                        filtered_messages.append(msg)
                    
                    if filtered_messages:
                        logger.info(f"📥 [{username}] After filtering: {len(filtered_messages)} new message(s) to process")
                        
                        # Add messages to grouper
                        for msg in filtered_messages:
                            message_grouper.add_message(msg['room_id'], msg)
                            logger.debug(f"📨 [{username}] Added message to room {msg['room_id'][:8]} from {msg['username']}")
                        
                        # Mark messages as in progress IMMEDIATELY
                        for msg in filtered_messages:
                            msg_key = f"{msg['room_id']}_{msg['message_id']}"
                            messages_in_progress.add(msg_key)
                else:
                    # No new messages found - don't process anything
                    logger.debug(f"📭 [{username}] No new messages found from sync")
                    
                    # Force process any pending messages that are old enough (5+ seconds)
                    ready_groups = {}
                    for room_id in list(message_grouper.pending_messages.keys()):
                        if room_id in message_grouper.last_message_time:
                            time_since_last = current_time - message_grouper.last_message_time[room_id]
                            if time_since_last >= 5:  # 5 second timeout for grouping
                                messages = message_grouper.pending_messages.get(room_id, [])
                                if messages:
                                    ready_groups[room_id] = messages.copy()
                                    logger.info(f"⏰ [{username}] Forcing processing of {len(messages)} messages in room {room_id[:8]} (age: {time_since_last:.1f}s)")
                    
                    # Process each group
                    for room_id, messages in ready_groups.items():
                        if not messages:
                            continue
                        
                        # Remove from pending messages
                        if room_id in message_grouper.pending_messages:
                            del message_grouper.pending_messages[room_id]
                        if room_id in message_grouper.last_message_time:
                            del message_grouper.last_message_time[room_id]
                        
                        user_id = messages[0]['sender']
                        reddit_username = messages[0]['username']
                        
                        # Check for mod messages
                        if any(bot.matrix_chat.is_mod_ban_message(msg['message'], msg.get('sender')) for msg in messages):
                            logger.info(f"🚫 [{username}] Skipping mod/ban message from {reddit_username}")
                            for msg in messages:
                                bot.matrix_chat.persistent_state.mark_message_processed(msg['message_id'])
                                # Remove from in-progress tracking
                                msg_key = f"{msg['room_id']}_{msg['message_id']}"
                                if msg_key in messages_in_progress:
                                    messages_in_progress.remove(msg_key)
                            continue
                        
                        # DEBUG: Log what we're about to process
                        logger.info(f"🔧 [{username}] Generating response for {len(messages)} messages from {reddit_username} in room {room_id[:8]}")
                        
                        # Generate response with error handling
                        try:
                            response_result = bot.generate_response_for_grouped_messages(
                                room_id, messages, reddit_username, user_id
                            )
                            
                            # DEBUG: Log the response result
                            logger.debug(f"📝 [{username}] Response result type: {type(response_result)}")
                            if isinstance(response_result, tuple):
                                logger.debug(f"📝 [{username}] Response result length: {len(response_result)}")
                            
                            # Queue response for sending
                            self.response_queues[username].put({
                                'room_id': room_id,
                                'response_result': response_result,
                                'messages': messages,
                                'username': reddit_username
                            })
                            
                            logger.info(f"✅ [{username}] Queued response for {reddit_username}")
                            
                        except Exception as e:
                            logger.error(f"❌ [{username}] Error generating response for {reddit_username}: {e}")
                            import traceback
                            logger.error(f"❌ [{username}] Traceback: {traceback.format_exc()}")
                            # Mark messages as processed to avoid getting stuck
                            for msg in messages:
                                bot.matrix_chat.persistent_state.mark_message_processed(msg['message_id'])
                                # Remove from in-progress tracking
                                msg_key = f"{msg['room_id']}_{msg['message_id']}"
                                if msg_key in messages_in_progress:
                                    messages_in_progress.remove(msg_key)
                
                # Clean up old in-progress messages (older than 30 seconds)
                # This handles cases where messages never get properly marked as done
                messages_to_remove = []
                for msg_key in list(messages_in_progress):
                    # Extract timestamp from message key if possible
                    # If we can't determine age, we'll just keep it for now
                    pass
                
                for msg_key in messages_to_remove:
                    messages_in_progress.remove(msg_key)
                    logger.debug(f"🧹 [{username}] Removed stale message from in-progress tracking: {msg_key[:30]}")
                
                # Short sleep to prevent tight loop
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ [{username}] Message checker error: {e}")
                import traceback
                logger.error(f"❌ [{username}] Traceback: {traceback.format_exc()}")
                time.sleep(5)
        
        logger.info(f"🛑 [{username}] Message checker thread stopped")
    
    def _bot_response_sender_loop(self, username: str, bot: RedditChatBot):
        """Continuous response sending loop (processes queue) - WITH UNPACKING SAFETY"""
        logger.info(f"💬 [{username}] Response sender thread started")
        
        # Get access to the message checker's in-progress tracking
        if not hasattr(self, 'shared_in_progress'):
            self.shared_in_progress = {}
        if username not in self.shared_in_progress:
            self.shared_in_progress[username] = set()
        
        while self.is_running:
            try:
                try:
                    task = self.response_queues[username].get(timeout=0.5)
                    logger.info(f"📤 [{username}] Processing task from queue")
                except queue.Empty:
                    continue
                
                room_id = task['room_id']
                response_result = task['response_result']
                messages = task['messages']
                reddit_username = task['username']
                
                logger.info(f"📤 [{username}] Sending response to {reddit_username} in room {room_id[:8]}")
                
                # DEBUG: Log the response result
                logger.debug(f"📝 [{username}] Response result: {response_result}")
                
                # Check for special termination statuses FIRST
                if response_result == "CONVERSATION_ALREADY_TERMINATED":
                    logger.info(f"⏹️  [{username}] Conversation already terminated, ignoring messages from {reddit_username}")
                    # Mark messages as processed but don't send anything
                    for msg in messages:
                        bot.matrix_chat.mark_message_as_processed_and_update_cursor(
                            room_id, msg['message_id'], msg['timestamp']
                        )
                        # Remove from in-progress tracking
                        msg_key = f"{msg['room_id']}_{msg['message_id']}"
                        if msg_key in self.shared_in_progress[username]:
                            self.shared_in_progress[username].remove(msg_key)
                    continue
                
                if response_result == "CONVERSATION_TERMINATED_MOD_MESSAGE":
                    logger.info(f"🚫 [{username}] Ended conversation with {reddit_username} due to mod message")
                    for msg in messages:
                        bot.matrix_chat.mark_message_as_processed_and_update_cursor(
                            room_id, msg['message_id'], msg['timestamp']
                        )
                        # Remove from in-progress tracking
                        msg_key = f"{msg['room_id']}_{msg['message_id']}"
                        if msg_key in self.shared_in_progress[username]:
                            self.shared_in_progress[username].remove(msg_key)
                    continue
                
                # FIXED: Safe unpacking with type checking
                if isinstance(response_result, tuple):
                    # Handle tuple responses
                    if len(response_result) == 4:
                        response, image_info, should_continue, random_image_info = response_result
                    elif len(response_result) == 3:
                        response, image_info, should_continue = response_result
                        random_image_info = None
                    else:
                        logger.error(f"❌ [{username}] Unexpected tuple length: {len(response_result)}")
                        # Remove from in-progress tracking even on error
                        for msg in messages:
                            msg_key = f"{msg['room_id']}_{msg['message_id']}"
                            if msg_key in self.shared_in_progress[username]:
                                self.shared_in_progress[username].remove(msg_key)
                        continue
                    
                    # Check if response is a termination message
                    if isinstance(response, str) and response.startswith("CONVERSATION"):
                        logger.info(f"⏹️  [{username}] Conversation termination: {response}")
                        for msg in messages:
                            bot.matrix_chat.mark_message_as_processed_and_update_cursor(
                                room_id, msg['message_id'], msg['timestamp']
                            )
                            # Remove from in-progress tracking
                            msg_key = f"{msg['room_id']}_{msg['message_id']}"
                            if msg_key in self.shared_in_progress[username]:
                                self.shared_in_progress[username].remove(msg_key)
                        continue
                else:
                    logger.error(f"❌ [{username}] Unexpected response type: {type(response_result)}")
                    # Remove from in-progress tracking even on error
                    for msg in messages:
                        msg_key = f"{msg['room_id']}_{msg['message_id']}"
                        if msg_key in self.shared_in_progress[username]:
                            self.shared_in_progress[username].remove(msg_key)
                    continue
                
                # FIXED: Safe unpacking of random_image_info
                random_image_path_or_url = None
                random_is_cached_url = False
                
                if random_image_info is not None:
                    if isinstance(random_image_info, tuple) and len(random_image_info) == 2:
                        random_image_path_or_url, random_is_cached_url = random_image_info
                    else:
                        logger.warning(f"⚠️ [{username}] random_image_info is not a 2-tuple: {type(random_image_info)}")
                        random_image_path_or_url = None
                        random_is_cached_url = False
                
                # DEBUG: Log what we're about to send
                logger.info(f"💬 [{username}] Sending message to {reddit_username}: {response[:100]}...")
                
                # === ADD 3-SECOND DELAY HERE BEFORE SENDING RESPONSE ===
                logger.info(f"⏰ [{username}] Adding 3-second delay before sending response to {reddit_username}")
                time.sleep(3)
                
                # Send the response
                if bot.matrix_chat.send_message(room_id, response):
                    logger.info(f"✅ [{username}] Message sent successfully to {reddit_username}")
                    
                    if should_continue:
                        current_count = bot.message_counts.get(room_id, 0)
                        remaining = 30 - current_count
                        
                        logger.info(f"✅ [{username}] Replied to {len(messages)} messages from {reddit_username}")
                        logger.info(f"📊 [{username}] Progress: {current_count}/30 messages (Remaining: {remaining})")
                        
                        # Mark messages as processed
                        for msg in messages:
                            bot.matrix_chat.mark_message_as_processed_and_update_cursor(
                                room_id, msg['message_id'], msg['timestamp']
                            )
                            # Remove from in-progress tracking
                            msg_key = f"{msg['room_id']}_{msg['message_id']}"
                            if msg_key in self.shared_in_progress[username]:
                                self.shared_in_progress[username].remove(msg_key)
                        
                        # Send regular image if we have one
                        if image_info:
                            image_path_or_url, is_cached_url = image_info
                            if image_path_or_url and not is_cached_url:
                                time.sleep(1)  # Short pause before image
                                logger.info(f"📸 [{username}] Sending image to {reddit_username}")
                                
                                # FIXED: Use chat interface for image sending with matrix token
                                if hasattr(bot.matrix_chat, 'chat_interface'):
                                    # Navigate to the room
                                    if bot.matrix_chat.chat_interface.navigate_to_room(room_id):
                                        # Pass matrix token from bot.matrix_chat
                                        matrix_token = bot.matrix_chat.matrix_token
                                        success = bot.matrix_chat.chat_interface.send_image_to_current_room(image_path_or_url, matrix_token)
                                    else:
                                        logger.error(f"❌ [{username}] Failed to navigate to room for image sending")
                                        success = False
                                
                                if success:
                                    logger.info(f"✅ [{username}] Image sent successfully to {reddit_username}")
                                else:
                                    logger.warning(f"⚠️ [{username}] Failed to send image to {reddit_username}")
                        
                        # Send random image for Phase 2 if we have one
                        if random_image_path_or_url and not random_is_cached_url:
                            time.sleep(1)  # Pause
                            logger.info(f"🎲 [{username}] Sending random image to {reddit_username}")
                            
                            # FIXED: Use chat interface for random images with matrix token
                            if hasattr(bot.matrix_chat, 'chat_interface'):
                                # Navigate to room first
                                if bot.matrix_chat.chat_interface.navigate_to_room(room_id):
                                    # Pass matrix token from bot.matrix_chat
                                    matrix_token = bot.matrix_chat.matrix_token
                                    image_success = bot.matrix_chat.chat_interface.send_image_to_current_room(random_image_path_or_url, matrix_token)
                                else:
                                    logger.error(f"❌ [{username}] Failed to navigate to room for random image sending")
                                    image_success = False
                            
                            if image_success:
                                logger.info(f"✅ [{username}] Random image sent successfully to {reddit_username}")
                            else:
                                logger.warning(f"⚠️ [{username}] Failed to send random image to {reddit_username}")
                        
                        # Update conversation history
                        bot.matrix_chat.update_conversation_history(room_id, messages[0]['room_name'])
                    
                    else:
                        # End conversation - send termination message
                        logger.info(f"⏹️  [{username}] Ended conversation with {reddit_username}")
                        for msg in messages:
                            bot.matrix_chat.mark_message_as_processed_and_update_cursor(
                                room_id, msg['message_id'], msg['timestamp']
                            )
                            # Remove from in-progress tracking
                            msg_key = f"{msg['room_id']}_{msg['message_id']}"
                            if msg_key in self.shared_in_progress[username]:
                                self.shared_in_progress[username].remove(msg_key)
                        bot.matrix_chat.conversation_manager.mark_conversation_completed(messages[0]['sender'])
                        # IMPORTANT: Also mark room as terminated locally
                        if room_id not in bot.terminated_conversations:
                            bot.terminated_conversations.add(room_id)
                
                else:
                    logger.error(f"❌ [{username}] Failed to send reply to {reddit_username}")
                    # Try one more time
                    time.sleep(2)
                    if bot.matrix_chat.send_message(room_id, response):
                        logger.info(f"✅ [{username}] Message sent successfully on retry")
                        # Remove from in-progress tracking on success
                        for msg in messages:
                            msg_key = f"{msg['room_id']}_{msg['message_id']}"
                            if msg_key in self.shared_in_progress[username]:
                                self.shared_in_progress[username].remove(msg_key)
                    else:
                        logger.error(f"❌ [{username}] Failed to send reply even on retry")
                        # Still remove from in-progress to prevent blocking
                        for msg in messages:
                            msg_key = f"{msg['room_id']}_{msg['message_id']}"
                            if msg_key in self.shared_in_progress[username]:
                                self.shared_in_progress[username].remove(msg_key)
                
            except Exception as e:
                logger.error(f"❌ [{username}] Response sender error: {e}")
                import traceback
                logger.error(f"❌ [{username}] Traceback: {traceback.format_exc()}")
                # Clean up any in-progress messages on error
                if 'messages' in locals():
                    for msg in messages:
                        msg_key = f"{msg['room_id']}_{msg['message_id']}"
                        if msg_key in self.shared_in_progress[username]:
                            self.shared_in_progress[username].remove(msg_key)
        
        logger.info(f"🛑 [{username}] Response sender thread stopped")
    
    def _bot_outgoing_sender_loop(self, username: str, bot: RedditChatBot):
        """Outgoing message loop - MODIFIED TO DO NOTHING"""
        logger.info(f"📤 [{username}] Outgoing sender thread DISABLED")
        
        while self.is_running:
            try:
                # DO NOTHING - Outgoing messages are disabled
                logger.debug(f"📭 [{username}] Outgoing messages disabled")
                time.sleep(180)  # Still sleep to prevent tight loop
                
            except Exception as e:
                logger.error(f"❌ [{username}] Outgoing sender error: {e}")
                time.sleep(60)
        
        logger.info(f"🛑 [{username}] Outgoing sender thread stopped")
        
    def _bot_invite_checker_loop(self, username: str, bot: RedditChatBot):
        """Periodic invite checking loop (runs every 30 seconds)"""
        logger.info(f"🚪 [{username}] Invite checker thread started")  # FIXED: Use username parameter
        
        while self.is_running:
            try:
                # Check invites
                bot.check_invites_cycle()
                
                # Wait 30 seconds
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ [{username}] Invite checker error: {e}")
                time.sleep(30)
        
        logger.info(f"🛑 [{username}] Invite checker thread stopped")
        
    def _bot_legacy_room_checker_loop(self, username: str, bot: RedditChatBot):
        """Periodic legacy room checker loop - MODIFIED TO DO NOTHING"""
        logger.info(f"🔍 [{username}] Legacy room checker thread DISABLED")
        
        while self.is_running:
            try:
                # DO NOTHING - Outgoing messages are disabled
                logger.debug(f"📭 [{username}] Outgoing messages disabled")
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"❌ [{username}] Legacy room checker error: {e}")
                time.sleep(30)
        
        logger.info(f"🛑 [{username}] Legacy room checker thread stopped")
        
    def stop_all_bots(self):
        """Stop all bots gracefully"""
        self.is_running = False
        logger.info("🛑 Stopping all bots...")
        
        # Wait for threads to finish
        for username, thread_dict in self.threads.items():
            for thread_name, thread in thread_dict.items():
                if thread.is_alive():
                    thread.join(timeout=5)
        
        logger.info("✅ All bots stopped")
    
    def get_status(self):
        """Get status of all bots"""
        status = {}
        for username, bot in self.bots.items():
            try:
                # Get processed message count from database
                processed_count = 0
                if hasattr(bot, 'matrix_chat') and bot.matrix_chat:
                    with db_manager.get_cursor() as cursor:
                        cursor.execute("""
                            SELECT COUNT(*) as count FROM processed_messages 
                            WHERE account_username = %s
                        """, (username,))
                        result = cursor.fetchone()
                        processed_count = result['count'] if result else 0
                
                status[username] = {
                    'threads_alive': {name: thread.is_alive() for name, thread in self.threads.get(username, {}).items()},
                    'queue_size': self.response_queues[username].qsize() if username in self.response_queues else 0,
                    'conversations': len(bot.conversation_histories),
                    'processed_messages': processed_count
                }
            except Exception as e:
                logger.error(f"❌ Error getting status for {username}: {e}")
                status[username] = {
                    'threads_alive': {},
                    'queue_size': 0,
                    'conversations': 0,
                    'processed_messages': 0
                }
        return status
            
            

# ========== PHASE 1 FILTERS ==========
class Phase1Filter:
    """Filters for Phase 1 messages to make them less bot-like"""
    
    def __init__(self, api_key: str, account_username: str):
        self.api_key = api_key
        self.account_username = account_username
        self.base_url = "https://api.venice.ai/api/v1"
        self.user_name_received = False
        self.user_name = None
    
    def set_user_name(self, name: str):
        """Set the user's name and flag that we've received it"""
        if name and name.strip():
            self.user_name = name.strip()
            self.user_name_received = True
            logger.info(f"📝 [{self.account_username}] User name set: {self.user_name}")
    
    def filter_dm_questions(self, message: str) -> str:
        """Replace 'why did you DM me' type questions with engaging follow-ups in Phase 1 messages"""
        try:
            logger.info(f"[DEBUG PHASE1 FILTER] Starting DM question replacement")
            logger.info(f"[DEBUG ORIGINAL MESSAGE]: {message}")
            
            # First check if the message contains DM questions that need replacement
            dm_question_patterns = [
                r"what brings you to chat with me",
                r"why did you (?:dm|message) me",
                r"what brings you to my messages",
                r"what made you reach out",
                r"why are you messaging me",
                r"what brings you here",
                r"how can I help you today",
                r"what can I do for you",
                r"what brings you to my DMs",
                r"what brings you to my profile",
                r"why did you check out my profile",
            ]
            
            # Check if the message contains any DM questions
            has_dm_question = False
            for pattern in dm_question_patterns:
                if re.search(pattern, message, re.IGNORECASE):
                    has_dm_question = True
                    logger.info(f"[DEBUG FOUND DM QUESTION]: Pattern matched: {pattern}")
                    break
            
            if not has_dm_question:
                logger.info(f"[DEBUG NO DM QUESTION FOUND]: No replacement needed")
                return message
            
            # Get conversation context to make the replacement relevant
            # Since we don't have full context here, we'll use the message itself
            # and ask Venice AI to replace with something engaging
            
            prompt = f'''Rewrite this message by replacing any part that asks "what brings you to chat with me" or similar DM questions with an engaging, flirty follow-up question.
            
            IMPORTANT: DO NOT just remove the question. REPLACE it with something else that keeps the conversation going naturally.
            
            Original message: "{message}"
            
            Rules for replacement:
            1. Replace DM questions with flirty, engaging follow-ups
            2. Keep the friendly tone and emojis
            3. Ask about their interests, what they're into, or make a playful comment
            4. Keep it 2-3 sentences total
            5. Maintain any compliments or other parts of the original message
            6. Make it sound natural and conversational
            
            Examples:
            - Instead of "What brings you to chat with me?" use "I love connecting with people who share my interests! What kind of content do you enjoy most? 😊"
            - Instead of "Why did you message me?" use "I'm always excited when someone interesting reaches out! Tell me what you're into 😉"
            
            Rewritten message:'''
            
            payload = {
                "model": "venice-uncensored",
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 250,
                "temperature": 0.7
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            logger.info(f"🛡️  [{self.account_username}] Replacing DM questions with engaging follow-ups")
            logger.info(f"[DEBUG SENDING TO VENICE FOR REPLACEMENT]: Prompt length: {len(prompt)}")
            
            response = requests.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                ai_response = data['choices'][0]['message']['content'].strip()
                
                logger.info(f"[DEBUG VENICE REPLACEMENT RAW RESPONSE]: {ai_response}")
                
                # Clean up the response
                replaced_text = self.clean_ai_response(ai_response, message)
                
                # Safety check: if replacement is too short or same as original, use a good fallback
                if (len(replaced_text) < len(message) * 0.5 or  # Too short
                    replaced_text == message or  # No change
                    "what brings you" in replaced_text.lower()):  # Still has DM question
                    
                    logger.warning(f"[DEBUG REPLACEMENT ISSUE]: Using fallback replacement")
                    # Use one of our pre-defined good replacements
                    replaced_text = self._get_fallback_replacement(message)
                
                logger.info(f"✅ [{self.account_username}] Replaced DM question with engaging follow-up")
                logger.info(f"[DEBUG REPLACEMENT APPLIED]: BEFORE: {message}")
                logger.info(f"[DEBUG REPLACEMENT APPLIED]: AFTER: {replaced_text}")
                logger.info(f"[DEBUG LENGTH CHANGE]: {len(message)} chars → {len(replaced_text)} chars")
                
                return replaced_text
            else:
                logger.error(f"❌ [{self.account_username}] DM question replacement failed: {response.status_code}")
                logger.error(f"[DEBUG REPLACEMENT ERROR]: Response: {response.text[:200]}")
                # Fallback to our own replacement
                return self._get_fallback_replacement(message)
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] DM question replacement error: {e}")
            logger.error(f"[DEBUG REPLACEMENT EXCEPTION]: {e}")
            # Fallback to our own replacement
            return self._get_fallback_replacement(message)
    
    def _get_fallback_replacement(self, original_message: str) -> str:
        """Get a good fallback replacement when Venice AI fails"""
        # Remove DM questions and add engaging follow-ups
        dm_question_patterns = [
            r"\s*[.,]?\s*(?:what brings you to chat with me|why did you (?:dm|message) me|what brings you to my messages).*?\??",
            r"\s*[.,]?\s*(?:what made you reach out|why are you messaging me|what brings you here).*?\??",
            r"\s*[.,]?\s*(?:how can I help you today|what can I do for you).*?\??",
        ]
        
        # Try to remove the DM question part
        cleaned = original_message
        for pattern in dm_question_patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
        
        # Clean up punctuation
        cleaned = cleaned.strip()
        if cleaned.endswith(('.', '!', '?', ',')):
            cleaned = cleaned.rstrip('.,')
        
        # Add engaging follow-up questions
        follow_up_options = [
            " What are you into? 😊",
            " I'd love to know what you're looking for! 😉",
            " What kind of content gets you going? 🔥",
            " I'm curious what interests you the most! 😘",
            " Tell me what turns you on! 😈",
            " What's your favorite kind of content to see? 💕",
        ]
        
        # Choose a follow-up based on the tone of the original message
        original_lower = original_message.lower()
        if any(word in original_lower for word in ['horny', 'turned on', 'sexy', 'hot']):
            follow_up = " What's getting you all worked up? 😈"
        elif any(word in original_lower for word in ['nice', 'good', 'great', 'awesome']):
            follow_up = " So what are you in the mood for today? 😊"
        else:
            follow_up = random.choice(follow_up_options)
        
        # Combine cleaned message with follow-up
        if cleaned:
            # If we still have content after removing DM question
            if not cleaned.endswith(('!', '?', '...')):
                cleaned += "."
            return cleaned + follow_up
        else:
            # If everything was removed, use a completely new engaging message
            engaging_openers = [
                "I love when interesting people reach out! What are you into? 😊",
                "Thanks for messaging me! I'm always curious what brings cool people my way. What's on your mind? 😉",
                "Hey there! I enjoy connecting with people who share my interests. What kind of content do you like? 🔥",
                "Nice to meet you! I'm always excited to chat with new people. What are you looking for today? 😘",
            ]
            return random.choice(engaging_openers)
    
    def filter_user_name(self, message: str) -> str:
        """Remove user's name from Phase 1 messages if we've already received it - WITH DEBUG"""
        if not self.user_name_received or not self.user_name:
            logger.info(f"[DEBUG NAME FILTER SKIPPED]: No user name received yet or name not set")
            return message
        
        try:
            logger.info(f"[DEBUG NAME FILTER] Starting name filter for user: {self.user_name}")
            logger.info(f"[DEBUG ORIGINAL MESSAGE]: {message}")
            
            prompt = f'''Remove any mention of the name "{self.user_name}" from this message.
            If the name is not mentioned, return the original message exactly. If a name and the word "Hey" is mentioned, like "Hey Steve" remove both.
            
            Message: "{message}"
            
            Return ONLY the cleaned message, no other text:'''
            
            payload = {
                "model": "venice-uncensored",
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 200,
                "temperature": 0.3
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            logger.debug(f"🛡️  [{self.account_username}] Filtering user name from Phase 1 message")
            logger.info(f"[DEBUG SENDING TO VENICE FOR NAME FILTER]: Looking for name: {self.user_name}")
            
            response = requests.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=15
            )
            
            if response.status_code == 200:
                data = response.json()
                ai_response = data['choices'][0]['message']['content'].strip()
                
                logger.info(f"[DEBUG NAME FILTER RAW RESPONSE]: {ai_response}")
                
                # Use the cleaner method
                filtered_text = self.clean_ai_response(ai_response, message)
                
                if filtered_text != message:
                    logger.info(f"✅ [{self.account_username}] Removed user name '{self.user_name}' from Phase 1 message")
                    logger.info(f"[DEBUG NAME FILTER APPLIED]: BEFORE: {message}")
                    logger.info(f"[DEBUG NAME FILTER APPLIED]: AFTER: {filtered_text}")
                else:
                    logger.info(f"[DEBUG NAME NOT FOUND]: Name '{self.user_name}' not found in message, no change needed")
                
                return filtered_text
            else:
                logger.error(f"❌ [{self.account_username}] Name filter failed: {response.status_code}")
                logger.error(f"[DEBUG NAME FILTER ERROR]: Response: {response.text[:200]}")
                return message
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Name filter error: {e}")
            logger.error(f"[DEBUG NAME FILTER EXCEPTION]: {e}")
            return message
            
    def clean_ai_response(self, text: str, original_message: str) -> str:
        """Clean up AI response text to remove any prefixes and ensure it's valid"""
        if not text:
            return original_message
        
        cleaned_text = text.strip()
        
        # Remove quotes
        if cleaned_text.startswith('"') and cleaned_text.endswith('"'):
            cleaned_text = cleaned_text[1:-1]
        elif cleaned_text.startswith("'") and cleaned_text.endswith("'"):
            cleaned_text = cleaned_text[1:-1]
        
        # Remove common AI prefixes
        prefixes = ['Response:', 'Message:', 'Generated:', 'Answer:', 'Here:', 'Cleaned message:']
        for prefix in prefixes:
            if cleaned_text.lower().startswith(prefix.lower()):
                cleaned_text = cleaned_text[len(prefix):].strip()
        
        # Remove any remaining quotes
        if cleaned_text.startswith('"') and cleaned_text.endswith('"'):
            cleaned_text = cleaned_text[1:-1]
        
        # If cleaning resulted in empty text, return original
        if not cleaned_text or cleaned_text.isspace():
            return original_message
        
        return cleaned_text
    
    def extract_user_name(self, user_message: str) -> str:
        """Extract user's name from their message if mentioned"""
        # Simple extraction - look for patterns like "I'm [name]", "my name is [name]", etc.
        patterns = [
            r"i[''´`]?m\s+(\w+)",
            r"my\s+name\s+is\s+(\w+)",
            r"call\s+me\s+(\w+)",
            r"i\s+go\s+by\s+(\w+)",
            r"you\s+can\s+call\s+me\s+(\w+)"
        ]
        
        for pattern in patterns:
            match = re.search(pattern, user_message, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                if len(name) > 1 and name.lower() not in ['i', 'me', 'you', 'we', 'us']:
                    self.set_user_name(name)
                    return name
        
        return None        
        
# ========== BOUNDARY HANDLER ==========
class BoundaryHandler:
    """Handles boundary requests like social media requests and preview requests using Venice AI"""
    
    def __init__(self, api_key: str, account_username: str):
        self.api_key = api_key
        self.account_username = account_username
        self.base_url = "https://api.venice.ai/api/v1"
        
        # Social media triggers
        self.social_media_triggers = [
            "snapchat", "discord", "telegram", "instagram", "whatsapp",
            "video call", "video chat", "let's call", "phone number",
            "phone #", "kik", 
            "facetime", "skype"
        ]
        
        # Preview/Explicit triggers
        self.preview_triggers = [
            "preview", "sample", "free trial", "free sample",
            "show me your pussy", "let me see your pussy", "I want to see your pussy", "Show me your vagina",
            "Let me see your vagina", "Send pussy pics", "Send pussy", "Send me your pussy",
            "Show your pussy"
        ]
        
        logger.info(f"🛡️  [{self.account_username}] Boundary Handler initialized")
    
    def detect_boundary_request(self, message: str) -> tuple:
        """Detect if message contains boundary requests
        Returns: (request_type, triggers_found)
        """
        message_lower = message.lower()
        
        social_triggers_found = []
        preview_triggers_found = []
        
        # Check for social media requests
        for trigger in self.social_media_triggers:
            if trigger in message_lower:
                social_triggers_found.append(trigger)
        
        # Check for preview requests
        for trigger in self.preview_triggers:
            if trigger in message_lower:
                preview_triggers_found.append(trigger)
        
        if social_triggers_found and preview_triggers_found:
            return "both", list(set(social_triggers_found + preview_triggers_found))
        elif social_triggers_found:
            return "social", social_triggers_found
        elif preview_triggers_found:
            return "preview", preview_triggers_found
        
        return None, []
    
    def generate_boundary_response(self, message: str, request_type: str, triggers: list, 
                                   current_phase: str, conversation_context: str, 
                                   config: ChatBotConfig, out_of_images: bool = False) -> str:
        """Generate a natural boundary response using Venice AI - WITH DEBUG"""
        try:
            # Build the prompt based on request type and phase
            prompt = self._build_boundary_prompt(message, request_type, triggers, 
                                                 current_phase, conversation_context, 
                                                 config, out_of_images)
            
            # DEBUG: Log what we're sending to Venice AI for boundary handling
            logger.info(f"[DEBUG BOUNDARY HANDLER] Request type: {request_type}")
            logger.info(f"[DEBUG BOUNDARY TRIGGERS]: {triggers}")
            logger.info(f"[DEBUG CURRENT PHASE]: {current_phase}")
            logger.info(f"[DEBUG OUT OF IMAGES]: {out_of_images}")
            logger.info(f"[DEBUG PROMPT LENGTH]: {len(prompt)} chars")
            logger.info(f"[DEBUG PROMPT PREVIEW]: {prompt[:300]}...")
            
            payload = {
                "model": "venice-uncensored",
                "messages": [
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                "max_tokens": 200,
                "temperature": 0.7
            }
            
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            logger.info(f"🛡️  [{self.account_username}] Generating boundary response for {request_type} request in Phase {current_phase}")
            
            response = requests.post(
                f"{self.base_url}/chat/completions",
                json=payload,
                headers=headers,
                timeout=20
            )
            
            if response.status_code == 200:
                data = response.json()
                generated_text = data['choices'][0]['message']['content'].strip()
                
                logger.info(f"[DEBUG BOUNDARY RAW RESPONSE]: {generated_text}")
                
                # Clean up the response
                generated_text = self._clean_boundary_response(generated_text)
                
                logger.info(f"✅ [{self.account_username}] Generated boundary response: {generated_text[:100]}...")
                logger.info(f"[DEBUG FINAL BOUNDARY RESPONSE]: {generated_text}")
                
                return generated_text
            else:
                logger.error(f"❌ [{self.account_username}] Boundary response failed: {response.status_code}")
                logger.error(f"[DEBUG BOUNDARY ERROR]: Response: {response.text[:200]}")
                return self._get_fallback_response(request_type, current_phase, triggers, out_of_images)
                
        except Exception as e:
            logger.error(f"❌ [{self.account_username}] Error generating boundary response: {e}")
            logger.error(f"[DEBUG BOUNDARY EXCEPTION]: {e}")
            return self._get_fallback_response(request_type, current_phase, triggers, out_of_images)
    
    def _build_boundary_prompt(self, message: str, request_type: str, triggers: list,
                              current_phase: str, conversation_context: str,
                              config: ChatBotConfig, out_of_images: bool) -> str:
        """Build the prompt for Venice AI"""
        
        # Phase-specific instructions
        phase_instructions = {
            "intro": "Respond naturally and keep the conversation going on Reddit. Be flirty and engaging.",
            "flirty": "Respond naturally and keep the conversation going on Reddit. Be flirty and sexy.",
            "pitch": "Respond naturally and redirect to your OnlyFans. Include the link: onlyfans .com/rileyboo.tv"
        }
        
        # Request type descriptions
        request_descriptions = {
            "social": f"The user asked for your {', '.join(triggers[:2])} or wants to contact you off Reddit.",
            "preview": f"The user asked to see your {', '.join(triggers[:2])} or wants free explicit content.",
            "both": f"The user asked for {', '.join(triggers[:3])}."
        }
        
        # Special conditions
        special_conditions = []
        if out_of_images and request_type == "preview":
            special_conditions.append("You've already shown all your Reddit pictures.")
        if current_phase == "pitch":
            special_conditions.append("You're currently pitching your OnlyFans.")
        
        # Build the prompt
        prompt = f"""You are Riley, a 22 year old busty blonde college girl who posts nudes on Reddit.
        
YOUR PERSONA:
{config.get('main')}

BACKGROUND:
{config.get('background')}

CHATTING STYLE:
{config.get('chatting_style')}

CURRENT CONVERSATION PHASE: {current_phase.upper()}
{phase_instructions.get(current_phase, phase_instructions['intro'])}

REQUEST DETAILS:
{request_descriptions.get(request_type, 'The user made a boundary request.')}
User's message: "{message}"

SPECIAL CONDITIONS:
{chr(10).join(f"- {cond}" for cond in special_conditions) if special_conditions else "None"}

CONVERSATION CONTEXT (last few messages):
{conversation_context if conversation_context else "Start of conversation"}

RESPONSE REQUIREMENTS:
1. Stay in character as Riley
2. Politely set boundaries (you don't give out contact info/show explicit content on Reddit)
3. If in Phase 1 or 2: keep conversation going here on Reddit, be flirty
4. If in Phase 3 or out of images: naturally redirect to OnlyFans (use link if in Phase 3)
5. Keep response 2-3 sentences max
6. Include 1-2 emojis
7. Sound natural and conversational, not robotic

Your response:"""
        
        return prompt
    
    def _clean_boundary_response(self, response: str) -> str:
        """Clean up the boundary response"""
        import re
        
        # Remove quotes if present
        if response.startswith('"') and response.endswith('"'):
            response = response[1:-1].strip()
        elif response.startswith("'") and response.endswith("'"):
            response = response[1:-1].strip()
        
        # Remove AI response prefixes
        prefixes = ['Response:', 'Message:', 'Generated:', 'Answer:', 'Here:', 'Riley:']
        for prefix in prefixes:
            if response.lower().startswith(prefix.lower()):
                response = response[len(prefix):].strip()
        
        # Clean up extra whitespace
        response = re.sub(r'\s+', ' ', response).strip()
        
        return response
    
    def _get_fallback_response(self, request_type: str, current_phase: str, 
                              triggers: list, out_of_images: bool) -> str:
        """Get fallback response if Venice AI fails"""
        
        if request_type == "social":
            if current_phase in ["intro", "flirty"]:
                return f"I don't give out {triggers[0] if triggers else 'that'} for privacy, but I love chatting here! What else are you into? 😉"
            else:  # pitch phase
                return f"I only connect on OnlyFans for safety! You should check me out there: onlyfans .com/rileyboo.tv 🔥"
        
        elif request_type == "preview":
            if current_phase in ["intro", "flirty"] and not out_of_images:
                return f"I save my explicit content for special places, but I love teasing here! What turns you on? 😉"
            else:  # pitch phase or out of images
                return f"All my explicit content is on OnlyFans! That's where I show everything: onlyfans .com/rileyboo.tv 😈"
        
        else:  # both or unknown
            if current_phase in ["intro", "flirty"]:
                return "I keep things on Reddit for privacy, but we're having fun here! Tell me more 😊"
            else:
                return "For all that and more, check out my OnlyFans: onlyfans .com/rileyboo.tv! 😘"

def main():
    """Main function"""
    # Enable debug logging
    logging.getLogger().setLevel(logging.DEBUG)
    VENICE_API_KEY = "tTa2WtwBh_esrg0T-UDed2kdYjbUJkP7aeod7iA-Nc"
    
    print("🚀 Reddit ChatBot - CONCURRENT OPERATION")
    print("=" * 60)
    print("⚡ NEW: All functions run concurrently")
    print("📤 Message response time: 1-6 seconds (not 30+)")
    print("🔄 Each bot: 6 independent threads")
    print("⏰ Outgoing: Every 5 minutes per bot (independent)")
    print("🧵 No waiting between bots")
    print("📨 LEGACY SYSTEM: Accounts from accounts2.txt use old Reddit messaging")
    print("🔄 DUAL SYSTEM: Accounts from accounts3.txt use BOTH systems")
    print("=" * 60)
    
    account_manager = AccountManager()
    accounts = account_manager.get_accounts()
    
    # DEBUG: Print all account configurations
    debug_trace("Main: Account configurations loaded:")
    for username, config in accounts.items():
        debug_trace(f"  {username}: {config}")
    
    if not accounts:
        logger.error("❌ No accounts configured!")
        return
    
    logger.info(f"👥 Starting {len(accounts)} account bots CONCURRENTLY...")
    logger.info(f"⏱️  Staggered initialization with 30-second delays")
    
    # Use ConcurrentBotManager instead of ParallelBotManager
    concurrent_manager = ConcurrentBotManager()
    
    for i, (username, config) in enumerate(accounts.items()):
        logger.info(f"🚀 [{i+1}/{len(accounts)}] Initializing account: {username}")
        
        # Get the account type from configuration
        account_type = config.get('account_type', 'standard')
        
        logger.info(f"   📤 Message method: {account_type.upper()}")
        debug_trace(f"Main: Creating bot for {username} with account_type={account_type}")
        
        # Create bot
        bot = RedditChatBot(
            venice_api_key=VENICE_API_KEY,
            account_username=username,
            use_legacy_system=(account_type in ['legacy', 'both'])  # Set flag for legacy/both
        )
        
        if bot.initialize_matrix_chat_with_retry(
            config['proxy'],
            config['adspower_profile_id'],
            account_type=account_type,  # Pass account type
            max_retries=3,
            retry_delay=30
        ):
            logger.info(f"✅ Successfully initialized {username} ({account_type.upper()} method)")
            concurrent_manager.add_bot(username, bot)
            
            # Shorter delay between initializations
            if i < len(accounts) - 1:
                delay_seconds = 30  # Reduced from 60 seconds
                logger.info(f"⏱️  Waiting {delay_seconds} seconds before initializing next account...")
                for remaining in range(delay_seconds, 0, -5):
                    if remaining % 15 == 0 or remaining <= 5:
                        logger.info(f"   {remaining} seconds remaining...")
                    time.sleep(min(5, remaining))
                logger.info("✅ Delay complete, continuing with next account")
        else:
            logger.error(f"❌ Failed to initialize {username}")
            if i < len(accounts) - 1:
                delay_seconds = 30
                logger.info(f"⏱️  Waiting {delay_seconds} seconds before trying next account...")
                time.sleep(delay_seconds)
            continue
    
    if len(concurrent_manager.bots) == 0:
        logger.error("❌ No accounts successfully initialized!")
        return
    
    logger.info(f"🎯 Starting {len(concurrent_manager.bots)} bots CONCURRENTLY...")
    logger.info(f"⚡ Each bot runs 6 threads independently")
    logger.info(f"📤 Message latency: 1-6 seconds")
    logger.info(f"⏰ Outgoing messages flow continuously")
    
    # Display account methods summary
    standard_count = sum(1 for _, config in accounts.items() if config.get('account_type') == 'standard')
    legacy_count = sum(1 for _, config in accounts.items() if config.get('account_type') == 'legacy')
    both_count = sum(1 for _, config in accounts.items() if config.get('account_type') == 'both')
    
    logger.info(f"📊 Account Methods Summary:")
    logger.info(f"   📤 Standard: {standard_count}")
    logger.info(f"   📨 Legacy: {legacy_count}")
    logger.info(f"   🔄 Dual (BOTH): {both_count}")
    debug_trace(f"Main: Account summary - {standard_count} Standard, {legacy_count} Legacy, {both_count} Dual")
    
    try:
        # Start all bots concurrently
        concurrent_manager.start_all_bots()
        
        # Keep main thread alive, display status periodically
        status_interval = 60  # Show status every minute
        last_status_time = 0
        
        while True:
            current_time = time.time()
            if current_time - last_status_time >= status_interval:
                status = concurrent_manager.get_status()
                logger.info(f"📊 CONCURRENT STATUS UPDATE:")
                for username, bot_status in status.items():
                    threads_alive = sum(1 for alive in bot_status['threads_alive'].values() if alive)
                    
                    # Get account type
                    account_config = accounts.get(username, {})
                    account_type = account_config.get('account_type', 'standard')
                    
                    logger.info(f"   👤 {username} ({account_type.upper()}): {threads_alive}/6 threads alive, {bot_status['queue_size']} queued, {bot_status['conversations']} active convos")
                last_status_time = current_time
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("🛑 Stopping all bots...")
        concurrent_manager.stop_all_bots()
        logger.info("✅ All bots stopped gracefully")
    except Exception as e:
        logger.error(f"❌ Error in concurrent execution: {e}")
        concurrent_manager.stop_all_bots()
        
if __name__ == "__main__":
    main()