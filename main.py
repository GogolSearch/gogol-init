import argparse
import os
import time
import traceback
import uuid
import psycopg
import logging
import sys
import redis
import signal

from implementations.lock import RedisLock


# Helper function to fetch environment variables with optional default
def get_env_variable(name, default=None):
    """Fetch environment variable, fallback to default."""
    return os.environ.get(name, default)


# Parse command-line arguments
def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description='Database configuration')

    # Database arguments
    parser.add_argument(
        '--db_host',
        type=str,
        default=get_env_variable('DB_HOST')
    )

    parser.add_argument(
        '--db_name',
        type=str,
        default=get_env_variable('DB_NAME')
    )

    parser.add_argument(
        '--db_user',
        type=str,
        default=get_env_variable('DB_USER')
    )

    parser.add_argument(
        '--db_password',
        type=str,
        default=get_env_variable('DB_PASSWORD')
    )

    parser.add_argument(
        '--db_port',
        type=int,
        default=get_env_variable('DB_PORT')
    )

    parser.add_argument(
        "--log_level",
        type=str,
        default=get_env_variable('LOG_LEVEL', default="INFO")
    )
    # Redis arguments
    parser.add_argument(
        '--redis_host',
        type=str,
        default=get_env_variable('REDIS_HOST')
    )

    parser.add_argument(
        '--redis_port',
        type=int,
        default=get_env_variable('REDIS_PORT')
    )

    parser.add_argument(
        '--redis_db',
        type=int,
        default=get_env_variable('REDIS_DB', default=0)
    )

    parser.add_argument(
        "--lock_name",
        type=str,
        default=get_env_variable('LOCK_NAME', default="crawler:db_lock")
    )
    parser.add_argument(
        "--lock_timeout",
        type=int,
        default=get_env_variable('LOCK_TIMEOUT', default=900)
    )
    parser.add_argument(
        "--pagerank_delay",
        type=int,
        default=get_env_variable('PAGERANK_DELAY', default=10800)
    )

    # Parse and return the arguments
    return parser.parse_args()


# Function to create tables if they don't already exist
def init_database(cursor):
    """Create database tables if they don't already exist."""

    # Define the full SQL schema to create tables, indexes, constraints, functions, and views
    sql_commands = [
        """
        CREATE TABLE IF NOT EXISTS "public"."links" (
            "source_url_id" bigint NOT NULL,
            "destination_url_id" bigint NOT NULL,
            CONSTRAINT "links_source_destination_pk" PRIMARY KEY ("source_url_id", "destination_url_id")
        ) WITH (oids = false);
        """,
        """
        CREATE INDEX IF NOT EXISTS "links_destination_url_id" 
        ON "public"."links" USING btree ("destination_url_id");
        """,
        """
        CREATE INDEX IF NOT EXISTS "links_source_url_id" 
        ON "public"."links" USING btree ("source_url_id");
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."page_rank" (
            "url_id" bigint NOT NULL,
            "rank" numeric NOT NULL,
            CONSTRAINT "page_rank_url_id" PRIMARY KEY ("url_id")
        ) WITH (oids = false);
        """,
        """
        CREATE INDEX IF NOT EXISTS "page_rank_rank" 
        ON "public"."page_rank" USING btree ("rank");
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."pages" (
            "url_id" bigint NOT NULL,
            "title" text,
            "description" text,
            "content" text NOT NULL,
            "metadata" json,
            "created_at" timestamp DEFAULT now() NOT NULL,
            "icon" character varying(2048),
            CONSTRAINT "pages_pkey" PRIMARY KEY ("url_id")
        ) WITH (oids = false);
        """,
        """
        CREATE INDEX IF NOT EXISTS pages_idx 
        ON "public"."pages" USING bm25 (
            url_id, title, description, content
        ) WITH (key_field='url_id');
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."redirects" (
            "source_url_id" bigint NOT NULL,
            "destination_url_id" bigint NOT NULL,
            "redirect_type" integer NOT NULL,
            "created_at" timestamp DEFAULT now() NOT NULL,
            CONSTRAINT "redirects_source_url_id" PRIMARY KEY ("source_url_id")
        ) WITH (oids = false);
        """,
        """
        CREATE SEQUENCE IF NOT EXISTS urls_id_seq 
        INCREMENT 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1;
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."urls" (
            "id" bigint DEFAULT nextval('urls_id_seq') NOT NULL,
            "url" character varying(2048) NOT NULL,
            "failed_tries" smallint DEFAULT 0 NOT NULL,
            "last_crawled_at" timestamp,
            "queued" boolean DEFAULT false NOT NULL,
            "created_at" timestamp DEFAULT now() NOT NULL,
            "canonical_url_id" bigint,
            CONSTRAINT "urls_pkey" PRIMARY KEY ("id"),
            CONSTRAINT "urls_url_key" UNIQUE ("url")
        ) WITH (oids = false);
        """,
        """
        CREATE INDEX IF NOT EXISTS "urls_canonical_url_id" 
        ON "public"."urls" USING btree ("canonical_url_id");
        """,
        """
        CREATE INDEX IF NOT EXISTS "urls_idx" 
        ON "public"."urls" USING btree ("url");
        """,
        """
        CREATE OR REPLACE FUNCTION delete_url_on_failed_tries()
        RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.failed_tries >= 5 THEN
                DELETE FROM "public"."urls" WHERE id = NEW.id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
        """
        CREATE OR REPLACE TRIGGER trigger_delete_url_on_failed_tries
        AFTER UPDATE ON "public"."urls"
        FOR EACH ROW
        WHEN (NEW.failed_tries >= 5)
        EXECUTE FUNCTION delete_url_on_failed_tries();
        """,
        """
        ALTER TABLE "public"."links" 
        DROP CONSTRAINT IF EXISTS "links_destination_url_id_fkey";
        """,
        """
        ALTER TABLE "public"."links" 
        ADD CONSTRAINT "links_destination_url_id_fkey" 
        FOREIGN KEY (destination_url_id) REFERENCES urls(id) 
        ON UPDATE CASCADE ON DELETE CASCADE;
        """,
        """
        ALTER TABLE "public"."links" 
        DROP CONSTRAINT IF EXISTS "links_source_url_id_fkey";
        """,
        """
        ALTER TABLE "public"."links" 
        ADD CONSTRAINT "links_source_url_id_fkey" 
        FOREIGN KEY (source_url_id) REFERENCES urls(id) 
        ON UPDATE CASCADE ON DELETE CASCADE;
        """,
        """
        ALTER TABLE "public"."page_rank" 
        DROP CONSTRAINT IF EXISTS "page_rank_url_id_fkey";
        """,
        """
        ALTER TABLE "public"."page_rank" 
        ADD CONSTRAINT "page_rank_url_id_fkey" 
        FOREIGN KEY (url_id) REFERENCES urls(id) 
        ON UPDATE CASCADE ON DELETE CASCADE;
        """,
        """
        ALTER TABLE "public"."pages" 
        DROP CONSTRAINT IF EXISTS "pages_url_id_fkey";
        """,
        """
        ALTER TABLE "public"."pages" 
        ADD CONSTRAINT "pages_url_id_fkey" 
        FOREIGN KEY (url_id) REFERENCES urls(id) 
        ON UPDATE CASCADE ON DELETE CASCADE;
        """,
        """
        ALTER TABLE "public"."redirects" 
        DROP CONSTRAINT IF EXISTS "redirects_destination_url_id_fkey";
        """,
        """
        ALTER TABLE "public"."redirects" 
        ADD CONSTRAINT "redirects_destination_url_id_fkey" 
        FOREIGN KEY (destination_url_id) REFERENCES urls(id) 
        ON DELETE CASCADE;
        """,
        """
        ALTER TABLE "public"."redirects" 
        DROP CONSTRAINT IF EXISTS "redirects_source_url_id_fkey";
        """,
        """
        ALTER TABLE "public"."redirects" 
        ADD CONSTRAINT "redirects_source_url_id_fkey" 
        FOREIGN KEY (source_url_id) REFERENCES urls(id) 
        ON DELETE CASCADE;
        """,
        """
        ALTER TABLE "public"."urls" 
        DROP CONSTRAINT IF EXISTS "urls_canonical_url_id_fkey";
        """,
        """
        ALTER TABLE "public"."urls" 
        ADD CONSTRAINT "urls_canonical_url_id_fkey" 
        FOREIGN KEY (canonical_url_id) REFERENCES urls(id) 
        ON DELETE SET NULL;
        """,
        """
        CREATE OR REPLACE VIEW "public"."resolved_links" AS
        SELECT DISTINCT
            COALESCE(src_url.canonical_url_id, links.source_url_id) AS source_url_id,
            COALESCE(
                final_dst.canonical_url_id,
                redirect.destination_url_id,
                dst_url.canonical_url_id,
                dst_url.id
            ) AS destination_url_id
        FROM "public"."links" AS links
        LEFT JOIN "public"."urls" AS src_url ON links.source_url_id = src_url.id
        LEFT JOIN "public"."urls" AS dst_url ON links.destination_url_id = dst_url.id
        LEFT JOIN "public"."redirects" AS redirect ON links.destination_url_id = redirect.source_url_id
        LEFT JOIN "public"."urls" AS final_dst ON redirect.destination_url_id = final_dst.id;
        """,
        """
        CREATE OR REPLACE VIEW "public"."outgoing_links" AS
        SELECT source_url_id, count(destination_url_id) AS outbound_count
        FROM resolved_links
        GROUP BY source_url_id;
        """,
        """
        CREATE OR REPLACE PROCEDURE batch_insert_pages(p_pages json)
        LANGUAGE plpgsql AS $$
        BEGIN
            -- Step 1: Create Temporary Table to store page data
            CREATE TEMP TABLE page_data (
                url TEXT,
                old_url TEXT,
                title TEXT,
                description TEXT,
                content TEXT,
                icon CHARACTER VARYING(2048),
                metadata json,
                links json,
                redirect_type INTEGER,
                canonical_url TEXT
            );
        
            -- Step 2: Populate page_data from the input JSON
            INSERT INTO page_data (url, old_url, title, description, content, icon, metadata, links, redirect_type, canonical_url)
            SELECT DISTINCT ON (page->>'url') 
                page->>'url',
                page->>'old_url',
                page->>'title',
                page->>'description',
                page->>'content',
                page->>'icon',
                page->'metadata',
                page->'links',
                (page->>'redirect_type')::INTEGER,
                page->>'canonical_url'
            FROM json_array_elements(p_pages) AS page;
        
            -- Step 3: Insert new URLs into the urls table, and avoid duplication
            INSERT INTO urls (url, failed_tries, queued, last_crawled_at)
            SELECT DISTINCT pd.url, 0, false, NOW()
            FROM page_data pd
            ON CONFLICT (url) DO UPDATE
                SET failed_tries = EXCLUDED.failed_tries,
                    queued = EXCLUDED.queued,
                    last_crawled_at = EXCLUDED.last_crawled_at;
        
            -- Insert old URLs into the urls table if they don't already exist
            INSERT INTO urls (url)
            SELECT DISTINCT pd.old_url
            FROM page_data pd
            WHERE pd.old_url IS NOT NULL
            ON CONFLICT (url) DO NOTHING;
        
            -- Step 4: Handle redirect types (301, 302)
            INSERT INTO redirects (source_url_id, destination_url_id, redirect_type)
            SELECT DISTINCT ON(u1.id) u1.id, u2.id, pd.redirect_type
            FROM page_data pd
            JOIN urls u1 ON u1.url = pd.old_url
            JOIN urls u2 ON u2.url = pd.url
            WHERE pd.redirect_type IS NOT NULL
              AND u1.id != u2.id
            ON CONFLICT (source_url_id) DO UPDATE
                SET redirect_type = EXCLUDED.redirect_type, destination_url_id = EXCLUDED.destination_url_id;
        
            -- Delete any existing redirects for URLs without a current redirect
            DELETE FROM redirects
            WHERE source_url_id IN (
                SELECT u.id
                FROM page_data pd
                JOIN urls u ON pd.url = u.url
                WHERE pd.redirect_type IS NULL
            );
        
            -- Step 5: Handle canonical URLs
            INSERT INTO urls (url)
            SELECT pd.canonical_url
            FROM page_data pd
            WHERE pd.canonical_url IS NOT NULL
            ON CONFLICT (url) DO NOTHING;
        
            UPDATE urls
            SET canonical_url_id = u.id
            FROM page_data pd
            JOIN urls u ON u.url = pd.canonical_url
            WHERE urls.url = pd.url
                AND pd.canonical_url IS NOT NULL;
        
            UPDATE urls
            SET canonical_url_id = NULL
            WHERE url IN (
                SELECT pd.url
                FROM page_data pd
                WHERE pd.canonical_url IS NULL
            );
        
            -- Step 6: Delete links where the current page is the source (for overwriting)
            DELETE FROM links
            WHERE source_url_id IN (
                SELECT u.id
                FROM urls u
                JOIN page_data pd ON u.url = pd.url
            );
        
            -- Step 7: Insert new links into the urls table if they don't exist
            INSERT INTO urls (url)
            SELECT DISTINCT link_url
            FROM page_data pd
            CROSS JOIN json_array_elements_text(pd.links) AS link_url
            ON CONFLICT (url) DO NOTHING;
        
            -- Step 8: Insert relationships into the links table (source -> destination)
            INSERT INTO links (source_url_id, destination_url_id)
            SELECT u1.id AS source_url_id, u2.id AS destination_url_id
            FROM page_data pd
            CROSS JOIN json_array_elements_text(pd.links) AS link_url
            JOIN urls u1 ON u1.url = pd.url
            JOIN urls u2 ON u2.url = link_url
            ON CONFLICT (source_url_id, destination_url_id) DO NOTHING;
        
            -- Step 9: Insert or update page data into the pages table
            INSERT INTO pages (url_id, title, description, content, icon, metadata, created_at)
            SELECT DISTINCT ON (COALESCE(u_canonical.id, u.id)) COALESCE(u_canonical.id, u.id) AS url_id, pd.title, pd.description, pd.content, pd.icon, pd.metadata, NOW()
            FROM page_data pd
            JOIN urls u ON u.url = pd.url
            LEFT JOIN urls u_canonical ON u_canonical.url = pd.canonical_url
            ON CONFLICT (url_id) DO UPDATE
                SET title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    content = EXCLUDED.content,
                    icon = EXCLUDED.icon,
                    metadata = EXCLUDED.metadata;
        
            -- Step 10: Mark pages as processed by setting queued = false
            UPDATE urls
            SET queued = false,
                failed_tries = 0,
                last_crawled_at = NOW()
            WHERE url IN (
                SELECT pd.url
                FROM page_data pd
                UNION
                SELECT pd2.old_url
                FROM page_data pd2
                WHERE pd2.old_url IS NOT NULL
                UNION
                SELECT pd3.canonical_url
                FROM page_data pd3
                WHERE pd3.canonical_url IS NOT NULL
            );
        
            -- Clean up the temporary table
            DROP TABLE IF EXISTS page_data;
        END; 
        $$;
        """,
        """
        CREATE OR REPLACE PROCEDURE calculate_pagerank(
            d_param DOUBLE PRECISION DEFAULT 0.85,
            epsilon_param DOUBLE PRECISION DEFAULT 0.0001,
            max_iterations_param INT DEFAULT 100
        )
        LANGUAGE plpgsql AS
        $$
        DECLARE
            N INT;
            delta DOUBLE PRECISION := 1.0;
            iteration INT := 0;
        BEGIN
            -- 1) Calcul du nombre total de pages
            SELECT COUNT(*) INTO N
            FROM urls;
        
            -- 2) Création de la table temporaire pour le rang courant
            DROP TABLE IF EXISTS temp_pagerank;
            CREATE TEMP TABLE temp_pagerank AS
            SELECT id as url_id,
                   1.0 / N AS rank
            FROM urls;
        
            -- 3) Création de la table temporaire pour le rang de l'itération suivante
            DROP TABLE IF EXISTS temp_pagerank_next;
            CREATE TEMP TABLE temp_pagerank_next (
                url_id INT,
                rank DOUBLE PRECISION
            );
        
            -- 4) Boucle d'itération
            WHILE delta >= epsilon_param
              AND iteration < max_iterations_param
            LOOP
                -- Vidage de la table pour y calculer les nouveaux rangs
                TRUNCATE temp_pagerank_next;
        
                -- 4.a) Calcul des nouveaux rangs
                INSERT INTO temp_pagerank_next (url_id, rank)
                SELECT
                    rl.destination_url_id AS url_id,
                    (1 - d_param) / N + d_param * SUM(tp.rank / ol.outbound_count) AS rank
                FROM resolved_links rl
                JOIN temp_pagerank tp ON rl.source_url_id = tp.url_id
                JOIN outgoing_links ol ON rl.source_url_id = ol.source_url_id
                GROUP BY rl.destination_url_id;
        
                -- 4.b) Calcul du delta (somme des différences)
                SELECT SUM(ABS(tpn.rank - tp.rank)) INTO delta
                FROM temp_pagerank_next tpn
                JOIN temp_pagerank tp ON tpn.url_id = tp.url_id;
        
                -- 4.c) Recopie des nouveaux rangs dans la table de l'itération courante
                TRUNCATE temp_pagerank;   -- on vide la table existante
                INSERT INTO temp_pagerank (url_id, rank)
                SELECT url_id, rank
                FROM temp_pagerank_next;
        
                -- Incrémentation du compteur d'itérations
                iteration := iteration + 1;
            END LOOP;
        
            -- 5) Mise à jour de la table de PageRank finale
            TRUNCATE page_rank;
            INSERT INTO page_rank (url_id, rank)
            SELECT url_id, rank
            FROM temp_pagerank;
        
            -- 6) Nettoyage final des tables temporaires
            DROP TABLE IF EXISTS temp_pagerank;
            DROP TABLE IF EXISTS temp_pagerank_next;
        
            RAISE NOTICE 'PageRank calculated after % iterations with delta %', iteration, delta;
        END;
        $$;

        """
    ]
    for sql_command in sql_commands:
        logging.debug(f"Running command:\n{sql_command}\n")
        cursor.execute(sql_command)
        logging.info("Executed SQL command.")


def configure_logging(log_level) -> None:
    """Configure logging for the application."""
    levels = logging.getLevelNamesMapping()
    logging.basicConfig(level=levels.get(log_level, logging.INFO), stream=sys.stdout)


def cleanup(signal, frame, connection, redis_client, lock):
    """Cleanup function to close resources on signal interrupt or terminate."""
    logging.info("Received termination signal, closing resources.")

    # Release the lock if it's owned
    if lock.owned():
        lock.release()

    # Close the Redis connection
    redis_client.close()

    # Close the PostgreSQL connection
    if connection:
        connection.close()

    logging.info("Resources closed successfully.")
    sys.exit(0)


# Main function to create the app
def main():
    args = parse_args()
    config = vars(args)  # Convert Namespace to dict
    token = str(uuid.uuid1()) + "-init"

    # Check for missing required configurations
    for k, v in config.items():
        if v is None:
            raise ValueError(f"Key '{k}' is required, but not provided or found in environment variables.")

    configure_logging(config["log_level"])
    logging.info("Configuration loaded successfully.")

    connection = None

    # Database connection setup
    try:
        connection = psycopg.connect(
            conninfo=f"dbname={config['db_name']} user={config['db_user']} password={config['db_password']} "
                     f"host={config['db_host']} port={config['db_port']}"
        )
        logging.info("Connected to the PostgreSQL database.")

    except psycopg.Error:
        logging.error(f"Error connecting to the database: {traceback.format_exc()}")
        return

    # Create tables if they don't exist
    cursor = connection.cursor()
    try:
        init_database(connection)
        logging.info("Database tables ensured.")
        connection.commit()
    except psycopg.Error as e:
        logging.error(f"Error while executing SQL command:\n{traceback.format_exc()}")
        connection.rollback()
        connection.close()
        logging.info("Database connection closed.")
    finally:
        cursor.close()

    running = True
    try:
        redis_client = redis.Redis(
            host=config["redis_host"],
            port=config["redis_port"],
            db=config["redis_db"],
            encoding="utf-8",
            decode_responses=True
        )
        logging.info("Redis connection established.")
    except redis.exceptions.ConnectionError as e:
        logging.error(f"Error connecting to the Redis database: {traceback.format_exc()}")
        return

    lock = RedisLock(redis_client, lock_name=config["lock_name"])

    # Setup signal handling
    signal.signal(signal.SIGINT, lambda signal, frame: cleanup(signal, frame, connection, redis_client, lock))
    signal.signal(signal.SIGTERM, lambda signal, frame: cleanup(signal, frame, connection, redis_client, lock))

    while running:
        logging.info(f"Waiting {config['pagerank_delay']} seconds...")
        time.sleep(config['pagerank_delay'])
        if lock.owned():
            logging.debug("Owned lock, releasing it and waiting for crawlers to use it.")
            lock.release()
            time.sleep(30)
        cursor = connection.cursor()
        try:
            if lock.acquire(blocking=True, blocking_timeout=config["lock_timeout"], token=token):
                logging.info("Starting page rank calculation")
                start = time.perf_counter()
                cursor.execute(f"CALL calculate_pagerank()")
                end = time.perf_counter()
                delta = end - start
                logging.info(f"Page rank calculation took {delta} seconds.")
                connection.commit()
            else:
                logging.info("Lock was already held trying next time.")
        except psycopg.Error as e:
            logging.error(f"Error while executing SQL command:\n{traceback.format_exc()}")
            connection.rollback()
        except TimeoutError as e:
            logging.error(f"Lock error: {traceback.format_exc()}")
        finally:
            cursor.close()
            if lock.owned():
                lock.release()

    # Final cleanup, in case loop exits unexpectedly
    if lock.owned():
        lock.release()
    redis_client.close()
    connection.close()


if __name__ == "__main__":
    main()
