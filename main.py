import argparse
import os
import psycopg
import logging


# Helper function to fetch environment variables with optional default
def get_env_variable(name, default=None):
    """Fetch environment variable, fallback to default."""
    return os.environ.get(name, default)


# Parse command-line arguments
def parse_args() -> argparse.Namespace:
    """Parse and return command-line arguments."""
    parser = argparse.ArgumentParser(description='Crawler Database configuration')

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

    # Parse and return the arguments
    return parser.parse_args()


# Function to create tables if they don't already exist
def create_tables(connection):
    """Create database tables if they don't already exist."""
    cursor = connection.cursor()

    # Define the full SQL schema to create tables, indexes, constraints, functions, and views
    sql_commands = [
        """
        CREATE TABLE IF NOT EXISTS "public"."urls" (
            "id" BIGSERIAL PRIMARY KEY,
            "url" VARCHAR(2048) NOT NULL,
            "failed_tries" SMALLINT DEFAULT 0 NOT NULL,
            "last_crawled_at" TIMESTAMP,
            "queued" BOOLEAN DEFAULT FALSE NOT NULL,
            "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
            "canonical_url_id" BIGINT,
            CONSTRAINT "urls_url_key" UNIQUE ("url")
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS "urls_canonical_url_id" ON "public"."urls" USING btree ("canonical_url_id");
        CREATE INDEX IF NOT EXISTS "urls_idx" ON "public"."urls" USING btree ("url");
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."links" (
            "source_url_id" BIGINT NOT NULL,
            "destination_url_id" BIGINT NOT NULL,
            PRIMARY KEY ("source_url_id", "destination_url_id")
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS "links_source_url_id" ON "public"."links" USING btree ("source_url_id");
        CREATE INDEX IF NOT EXISTS "links_destination_url_id" ON "public"."links" USING btree ("destination_url_id");
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."page_rank" (
            "url_id" BIGINT NOT NULL,
            "rank" NUMERIC NOT NULL,
            PRIMARY KEY ("url_id")
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS "page_rank_rank" ON "public"."page_rank" USING btree ("rank");
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."pages" (
            "url_id" BIGINT NOT NULL,
            "title" TEXT,
            "description" TEXT,
            "content" TEXT NOT NULL,
            "metadata" JSON,
            "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
            "icon" VARCHAR(2048),
            PRIMARY KEY ("url_id")
        );
        """,
        """
        CREATE INDEX IF NOT EXISTS "pages_idx" ON "public"."pages" USING bm25 (url_id, title, description, content, icon, metadata, created_at) WITH (key_field='url_id');
        """,
        """
        CREATE TABLE IF NOT EXISTS "public"."redirects" (
            "source_url_id" BIGINT NOT NULL,
            "destination_url_id" BIGINT NOT NULL,
            "redirect_type" INTEGER NOT NULL,
            "created_at" TIMESTAMP DEFAULT NOW() NOT NULL,
            PRIMARY KEY ("source_url_id")
        );
        """,
        """
        CREATE OR REPLACE FUNCTION delete_url_on_failed_tries() RETURNS TRIGGER AS $$
        BEGIN
            IF NEW.failed_tries >= 5 THEN
                DELETE FROM "public"."urls" WHERE id = NEW.id;
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """,
        """
        CREATE TRIGGER trigger_delete_url_on_failed_tries
        AFTER UPDATE ON "public"."urls"
        FOR EACH ROW
        WHEN (NEW.failed_tries >= 5)
        EXECUTE FUNCTION delete_url_on_failed_tries();
        """,
        """
        ALTER TABLE ONLY "public"."links" ADD CONSTRAINT "links_destination_url_id_fkey" FOREIGN KEY (destination_url_id) REFERENCES urls(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE;
        ALTER TABLE ONLY "public"."links" ADD CONSTRAINT "links_source_url_id_fkey" FOREIGN KEY (source_url_id) REFERENCES urls(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE;
        ALTER TABLE ONLY "public"."page_rank" ADD CONSTRAINT "page_rank_url_id_fkey" FOREIGN KEY (url_id) REFERENCES urls(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE;
        ALTER TABLE ONLY "public"."pages" ADD CONSTRAINT "pages_url_id_fkey" FOREIGN KEY (url_id) REFERENCES urls(id) ON UPDATE CASCADE ON DELETE CASCADE NOT DEFERRABLE;
        ALTER TABLE ONLY "public"."redirects" ADD CONSTRAINT "redirects_destination_url_id_fkey" FOREIGN KEY (destination_url_id) REFERENCES urls(id) ON DELETE CASCADE NOT DEFERRABLE;
        ALTER TABLE ONLY "public"."redirects" ADD CONSTRAINT "redirects_source_url_id_fkey" FOREIGN KEY (source_url_id) REFERENCES urls(id) ON DELETE CASCADE NOT DEFERRABLE;
        ALTER TABLE ONLY "public"."urls" ADD CONSTRAINT "urls_canonical_url_id_fkey" FOREIGN KEY (canonical_url_id) REFERENCES urls(id) NOT DEFERRABLE;
        """,
        """
        CREATE VIEW IF NOT EXISTS "public"."resolved_links" AS
        SELECT DISTINCT
            COALESCE(src_url.canonical_url_id, links.source_url_id) AS source_url_id,
            COALESCE(final_dst.canonical_url_id, redirect.destination_url_id, dst_url.canonical_url_id, dst_url.id) AS destination_url_id
        FROM
            "public"."links" AS links
        LEFT JOIN "public"."urls" AS src_url ON links.source_url_id = src_url.id
        LEFT JOIN "public"."urls" AS dst_url ON links.destination_url_id = dst_url.id
        LEFT JOIN "public"."redirects" AS redirect ON links.destination_url_id = redirect.source_url_id
        LEFT JOIN "public"."urls" AS final_dst ON redirect.destination_url_id = final_dst.id;
        """,
        """
        CREATE VIEW IF NOT EXISTS "public"."outgoing_links" AS
        SELECT source_url_id, count(destination_url_id) AS outbound_count
        FROM resolved_links
        GROUP BY source_url_id;
        """,
        """
        CREATE OR REPLACE PROCEDURE batch_insert_pages(p_pages jsonb) LANGUAGE plpgsql AS $$
        BEGIN
            CREATE TEMP TABLE page_data (
                url TEXT,
                old_url TEXT,
                title TEXT,
                description TEXT,
                content TEXT,
                icon VARCHAR(2048),
                metadata JSONB,
                links JSONB,
                redirect_type INTEGER,
                canonical_url TEXT
            );

            INSERT INTO page_data (url, old_url, title, description, content, icon, metadata, links, redirect_type, canonical_url)
            SELECT DISTINCT ON (page->>'url') page->>'url', page->>'old_url', page->>'title', page->>'description', page->>'content', page->>'icon', page->'metadata', page->'links', (page->>'redirect_type')::INTEGER, page->>'canonical_url'
            FROM jsonb_array_elements(p_pages) AS page;

            INSERT INTO urls (url, failed_tries, queued, last_crawled_at)
            SELECT DISTINCT pd.url, 0, false, NOW()
            FROM page_data pd
            ON CONFLICT (url) DO UPDATE SET failed_tries = EXCLUDED.failed_tries, queued = EXCLUDED.queued, last_crawled_at = EXCLUDED.last_crawled_at;

            INSERT INTO redirects (source_url_id, destination_url_id, redirect_type)
            SELECT DISTINCT ON(u1.id) u1.id, u2.id, pd.redirect_type
            FROM page_data pd
            JOIN urls u1 ON u1.url = pd.old_url
            JOIN urls u2 ON u2.url = pd.url
            WHERE pd.redirect_type IS NOT NULL AND u1.id != u2.id
            ON CONFLICT (source_url_id) DO UPDATE SET redirect_type = EXCLUDED.redirect_type, destination_url_id = EXCLUDED.destination_url_id;

            DELETE FROM redirects WHERE source_url_id IN (
                SELECT u.id
                FROM page_data pd
                JOIN urls u ON pd.url = u.url
                WHERE pd.redirect_type IS NULL
            );

            INSERT INTO urls (url)
            SELECT pd.canonical_url
            FROM page_data pd
            WHERE pd.canonical_url IS NOT NULL
            ON CONFLICT (url) DO NOTHING;

            UPDATE urls SET canonical_url_id = u.id
            FROM page_data pd
            JOIN urls u ON u.url = pd.canonical_url
            WHERE urls.url = pd.url;
        END;
        $$;
        """
    ]

    # Execute each SQL command
    for sql_command in sql_commands:
        cursor.execute(sql_command)
        logging.info("Executed SQL command.")

    connection.commit()
    cursor.close()


# Main function to create the app
def create_app():
    args = parse_args()
    config = vars(args)  # Convert Namespace to dict

    # Check for missing required configurations
    for k, v in config.items():
        if v is None:
            raise ValueError(f"Key '{k}' is required, but not provided or found in environment variables.")

    # Log the configuration
    logging.basicConfig(level=logging.INFO)
    logging.info("Configuration loaded successfully.")
    logging.info(config)

    connection = None

    # Database connection setup
    try:
        connection = psycopg.connect(
            host=config['db_host'],
            database=config['db_name'],
            user=config['db_user'],
            password=config['db_password'],
            port=config['db_port']
        )
        logging.info("Connected to the PostgreSQL database.")

        # Create tables if they don't exist
        create_tables(connection)
        logging.info("Database tables ensured.")

    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

    finally:
        if connection:
            connection.close()
            logging.info("Database connection closed.")


if __name__ == "__main__":
    create_app()
