from datetime import date, datetime
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    MetaData,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

SCHEMA = 'content'
metadata_obj = MetaData(schema=SCHEMA)


class Base(DeclarativeBase):
    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    metadata = metadata_obj


class FilmWork(Base):
    __tablename__ = 'film_work'

    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    creation_date: Mapped[date | None] = mapped_column(Date)
    rating: Mapped[float | None] = mapped_column(
        Float,
        CheckConstraint(
            'rating >= 0 AND rating <= 100',
            name='rating_allowed_ranges',
        ),
    )
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Связи
    genres: Mapped[list['GenreFilmWork']] = relationship(
        back_populates='film_work',
    )
    persons: Mapped[list['PersonFilmWork']] = relationship(
        back_populates='film_work',
    )

    # Индексы
    __table_args__ = (
        Index('film_work_creation_date_idx', creation_date),
    )

    def __repr__(self) -> str:
        return self.title


class Person(Base):
    __tablename__ = 'person'

    full_name: Mapped[str] = mapped_column(String(255), nullable=False)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Связи
    film_works: Mapped[list['PersonFilmWork']] = relationship(
        back_populates='person',
    )

    def __repr__(self) -> str:
        return self.full_name


class PersonFilmWork(Base):
    __tablename__ = 'person_film_work'

    role: Mapped[str] = mapped_column(String(50), nullable=False)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Внешние ключи
    person_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey(f'{SCHEMA}.person.id', ondelete='CASCADE'),
    )
    film_work_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey(f'{SCHEMA}.film_work.id', ondelete='CASCADE'),
    )

    # Связи
    person: Mapped['Person'] = relationship(back_populates='film_works')
    film_work: Mapped['FilmWork'] = relationship(back_populates='persons')

    # Уникальный индекс
    __table_args__ = (
        UniqueConstraint(
            'film_work_id', 'person_id', 'role',
            name='film_work_person_role_idx',
        ),
    )

    def __repr__(self) -> str:
        return f'{self.person.full_name} - {self.role}'


class Genre(Base):
    __tablename__ = 'genre'

    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
        unique=True,
    )
    description: Mapped[str | None] = mapped_column(Text)
    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    modified: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Связи
    film_works: Mapped[list['GenreFilmWork']
                       ] = relationship(back_populates='genre')

    def __repr__(self) -> str:
        return self.name


class GenreFilmWork(Base):
    __tablename__ = 'genre_film_work'

    created: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    # Внешние ключи
    genre_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey(f'{SCHEMA}.genre.id', ondelete='CASCADE'),
    )
    film_work_id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey(f'{SCHEMA}.film_work.id', ondelete='CASCADE'),
    )

    # Связи
    genre: Mapped['Genre'] = relationship(back_populates='film_works')
    film_work: Mapped['FilmWork'] = relationship(back_populates='genres')

    # Уникальный индекс
    __table_args__ = (
        UniqueConstraint(
            'film_work_id', 'genre_id',
            name='film_work_genre_idx',
        ),
    )

    def __repr__(self) -> str:
        return self.genre.name
