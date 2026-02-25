#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import sys
import jwt
import hashlib
from datetime import datetime, timedelta
from typing import Optional
from pathlib import Path

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from passlib.context import CryptContext

# --- ensure project root on sys.path ---
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import DATA_DIR
from src.utils.vault_loader import load_vault_config_once


# =========================
# Database setup
# =========================

DATABASE_DIR = DATA_DIR / "api_db"
DATABASE_DIR.mkdir(parents=True, exist_ok=True)
DATABASE_URL = f"sqlite:///{DATABASE_DIR / 'users.db'}"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)
Base = declarative_base()


# =========================
# Password hashing (FIXED)
# =========================

pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
)

def _prehash_password(password: str) -> bytes:
    """
    SHA-256 pre-hash to avoid bcrypt 72-byte limit
    """
    return hashlib.sha256(password.encode("utf-8")).digest()


def get_password_hash(password: str) -> str:
    """
    Hash password using SHA-256 + bcrypt
    """
    prehashed = _prehash_password(password)
    return pwd_context.hash(prehashed)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify password using SHA-256 + bcrypt
    """
    prehashed = _prehash_password(plain_password)
    return pwd_context.verify(prehashed, hashed_password)


# =========================
# JWT Settings
# =========================

# Load Vault-backed environment once before reading secrets.
load_vault_config_once()

# SECURITY: SECRET_KEY must be provided via environment variable.
SECRET_KEY = os.environ.get("SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError("SECRET_KEY is required and must be set via environment variable")

# Use RS256 for stronger security (requires RSA key pair)
# For now, using HS256 with strong secret key
ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 * 7  # 7 days


# =========================
# Database Models
# =========================

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    login = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=True)
    hashed_password = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )


Base.metadata.create_all(bind=engine)


# =========================
# JWT helpers
# =========================

def create_access_token(
    data: dict,
    expires_delta: Optional[timedelta] = None
) -> str:
    to_encode = data.copy()

    expire = (
        datetime.utcnow() + expires_delta
        if expires_delta
        else datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    )

    to_encode.update({
        "exp": expire,
        "iat": datetime.utcnow()
    })

    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def decode_access_token(token: str) -> Optional[dict]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        return None
    except Exception:
        # PyJWT raises InvalidTokenError/PyJWTError, while other JWT libs use JWTError.
        # Keep this broad for compatibility across installed jwt packages.
        return None


# =========================
# Database helpers
# =========================

def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_user_by_login(db: Session, login: str) -> Optional[User]:
    return db.query(User).filter(User.login == login).first()


def get_user_by_email(db: Session, email: str) -> Optional[User]:
    return db.query(User).filter(User.email == email).first()


def create_user(
    db: Session,
    login: str,
    password: str,
    email: Optional[str] = None
) -> User:
    hashed_password = get_password_hash(password)

    user = User(
        login=login,
        email=email,
        hashed_password=hashed_password,
        is_active=True,
    )

    db.add(user)
    db.commit()
    db.refresh(user)
    return user


def authenticate_user(
    db: Session,
    login: str,
    password: str
) -> Optional[User]:
    user = get_user_by_login(db, login)
    if not user:
        return None

    if not user.is_active:
        return None

    if not verify_password(password, user.hashed_password):
        return None

    return user


def get_current_user_from_token(
    token: str,
    db: Session
) -> Optional[User]:
    payload = decode_access_token(token)
    if not payload:
        return None

    login = payload.get("sub")
    if not login:
        return None

    return get_user_by_login(db, login)
