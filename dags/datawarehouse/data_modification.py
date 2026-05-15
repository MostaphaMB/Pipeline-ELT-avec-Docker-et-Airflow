import logging

logger = logging.getLogger(__name__)
table = "yt_api"

def insert_rows(cur, conn, schema, row):
    """
    Insère une ligne ou la met à jour si elle existe déjà (UPSERT).
    Évite l'erreur 'UniqueViolation' dans Airflow.
    """
    try:
        if schema == "staging":
            video_id_key = "video_id"
            # Utilisation de ON CONFLICT pour gérer les doublons proprement
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (
                    "Video_ID", "Video_Title", "Upload_Date", "Duration", 
                    "Video_Views", "Likes_Count", "Comments_Count"
                )
                VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s)
                ON CONFLICT ("Video_ID") 
                DO UPDATE SET 
                    "Video_Title" = EXCLUDED."Video_Title",
                    "Video_Views" = EXCLUDED."Video_Views",
                    "Likes_Count" = EXCLUDED."Likes_Count",
                    "Comments_Count" = EXCLUDED."Comments_Count";
                """,
                row,
            )
        else:
            video_id_key = "Video_ID"
            cur.execute(
                f"""
                INSERT INTO {schema}.{table} (
                    "Video_ID", "Video_Title", "Upload_Date", "Duration", 
                    "Video_Type", "Video_Views", "Likes_Count", "Comments_Count"
                )
                VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s)
                ON CONFLICT ("Video_ID") 
                DO UPDATE SET 
                    "Video_Title" = EXCLUDED."Video_Title",
                    "Video_Views" = EXCLUDED."Video_Views",
                    "Likes_Count" = EXCLUDED."Likes_Count",
                    "Comments_Count" = EXCLUDED."Comments_Count";
                """,
                row,
            )

        conn.commit()
        logger.info(f"Successfully upserted row with Video_ID: {row.get(video_id_key)}")

    except Exception as e:
        # En cas d'erreur, on annule la transaction pour ne pas bloquer Postgres
        conn.rollback()
        v_id = row.get("video_id") or row.get("Video_ID")
        logger.error(f"Error upserting row with Video_ID {v_id}: {e}")
        raise e


def update_rows(cur, conn, schema, row):
    """
    Met à jour manuellement une ligne existante.
    """
    try:
        if schema == "staging":
            mapping = {
                "v_id": "video_id", "v_date": "publishedAt", "v_title": "title",
                "v_views": "viewCount", "v_likes": "likeCount", "v_comments": "commentCount"
            }
        else:
            mapping = {
                "v_id": "Video_ID", "v_date": "Upload_Date", "v_title": "Video_Title",
                "v_views": "Video_Views", "v_likes": "Likes_Count", "v_comments": "Comments_Count"
            }

        cur.execute(
            f"""
            UPDATE {schema}.{table}
            SET "Video_Title" = %({mapping['v_title']})s,
                "Video_Views" = %({mapping['v_views']})s, 
                "Likes_Count" = %({mapping['v_likes']})s, 
                "Comments_Count" = %({mapping['v_comments']})s
            WHERE "Video_ID" = %({mapping['v_id']})s AND "Upload_Date" = %({mapping['v_date']})s;
            """,
            row,
        )
        conn.commit()
        logger.info(f"Updated row with Video_ID: {row.get(mapping['v_id'])}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error updating row: {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):
    """
    Supprime les lignes spécifiées.
    """
    try:
        # Formatage des IDs pour la clause SQL IN ('id1', 'id2')
        formatted_ids = ", ".join(f"'{id}'" for id in ids_to_delete)
        
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN ({formatted_ids});
            """
        )
        conn.commit()
        logger.info(f"Deleted rows with Video_IDs: {formatted_ids}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error deleting rows: {e}")
        raise e