#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Modify deadline's callback schema.

Revision ID: 808787349f22
Revises: 3bda03debd04
Create Date: 2025-07-31 19:35:53.150465

"""

from __future__ import annotations

import json
from textwrap import dedent

import sqlalchemy as sa
from alembic import context, op

# revision identifiers, used by Alembic.
revision = "808787349f22"
down_revision = "3bda03debd04"
branch_labels = None
depends_on = None
airflow_version = "3.1.0"


_ASYNC_CALLBACK_CLASSNAME = "airflow.sdk.definitions.deadline.AsyncCallback"


def upgrade():
    """Replace deadline table's string callback and JSON callback_kwargs with JSON callback."""
    if context.is_offline_mode():
        # In offline (--sql) mode there is no live connection so data cannot be
        # migrated.  Delete existing rows so the NOT NULL column can be added
        # safely.  This matches the offline behaviour of migration 0094.
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the deadline table
            --  while in offline mode!  All rows in the deadline table will
            --  be deleted.
            ------------
            """)
        )
        op.execute("DELETE FROM deadline")
        with op.batch_alter_table("deadline", schema=None) as batch_op:
            batch_op.drop_column("callback")
            batch_op.drop_column("callback_kwargs")
            batch_op.add_column(sa.Column("callback", sa.JSON(), nullable=False))
        return

    conn = op.get_bind()

    rows = conn.execute(sa.text("SELECT id, callback, callback_kwargs FROM deadline")).fetchall()
    callback_data: dict = {}
    for row in rows:
        path = row[1] or ""
        kwargs = row[2]
        if isinstance(kwargs, str):
            kwargs = json.loads(kwargs) if kwargs else {}
        elif kwargs is None:
            kwargs = {}
        callback_data[row[0]] = {
            "__data__": {"path": path, "kwargs": kwargs},
            "__classname__": _ASYNC_CALLBACK_CLASSNAME,
            "__version__": 0,
        }

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("callback")
        batch_op.drop_column("callback_kwargs")
        batch_op.add_column(sa.Column("callback", sa.JSON(), nullable=True))

    deadline_table = sa.table("deadline", sa.column("id"), sa.column("callback", sa.JSON()))
    for row_id, serialized in callback_data.items():
        conn.execute(
            sa.update(deadline_table).where(deadline_table.c.id == row_id).values(callback=serialized)
        )

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.alter_column("callback", existing_type=sa.JSON(), nullable=False)


def downgrade():
    """Replace deadline table's JSON callback with string callback and JSON callback_kwargs."""
    if context.is_offline_mode():
        print(
            dedent("""
            ------------
            --  WARNING: Unable to migrate the data in the deadline table
            --  while in offline mode!  All rows in the deadline table will
            --  be deleted.
            ------------
            """)
        )
        op.execute("DELETE FROM deadline")
        with op.batch_alter_table("deadline", schema=None) as batch_op:
            batch_op.drop_column("callback")
            batch_op.add_column(sa.Column("callback_kwargs", sa.JSON(), nullable=True))
            batch_op.add_column(sa.Column("callback", sa.String(length=500), nullable=False))
        return

    conn = op.get_bind()

    rows = conn.execute(sa.text("SELECT id, callback FROM deadline")).fetchall()
    callback_data: dict = {}
    for row in rows:
        cb = row[1]
        if cb is None:
            callback_data[row[0]] = ("", {})
            continue
        if isinstance(cb, str):
            cb = json.loads(cb)
        cb_inner = cb.get("__data__", cb)
        callback_data[row[0]] = (cb_inner.get("path", ""), cb_inner.get("kwargs", {}))

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.drop_column("callback")
        batch_op.add_column(sa.Column("callback_kwargs", sa.JSON(), nullable=True))
        batch_op.add_column(sa.Column("callback", sa.String(length=500), nullable=True))

    deadline_table = sa.table(
        "deadline",
        sa.column("id"),
        sa.column("callback", sa.String(500)),
        sa.column("callback_kwargs", sa.JSON()),
    )
    for row_id, (path, kwargs) in callback_data.items():
        conn.execute(
            sa.update(deadline_table)
            .where(deadline_table.c.id == row_id)
            .values(callback=path, callback_kwargs=kwargs)
        )

    with op.batch_alter_table("deadline", schema=None) as batch_op:
        batch_op.alter_column("callback", existing_type=sa.String(length=500), nullable=False)
