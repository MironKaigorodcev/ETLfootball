from sqlalchemy import create_engine, Column, Integer, String, Float, Date, ForeignKey, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship

Base = declarative_base()

class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True)
    fbref_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    url = Column(String)
    
    squad_stats = relationship("SquadStat", back_populates="team")
    players = relationship("Player", back_populates="team")

class Player(Base):
    __tablename__ = 'players'
    id = Column(Integer, primary_key=True)
    fbref_id = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    position = Column(String)
    nationality = Column(String)
    team_id = Column(Integer, ForeignKey('teams.id'))
    
    team = relationship("Team", back_populates="players")
    stats = relationship("PlayerStat", back_populates="player")

class Match(Base):
    __tablename__ = 'matches'
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    home_team_id = Column(Integer, ForeignKey('teams.id'))
    away_team_id = Column(Integer, ForeignKey('teams.id'))
    home_score = Column(Integer)
    away_score = Column(Integer)
    competition = Column(String)
    round = Column(String)
    venue = Column(String)
    attendance = Column(Integer)
    
    # Relationships can be added if we want direct object access, 
    # but foreign keys are enough for basic linkage.
    
class SquadStat(Base):
    __tablename__ = 'squad_stats'
    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey('teams.id'))
    season = Column(String, nullable=False)
    competition = Column(String, nullable=False)
    
    # Common stats (Defense, Possession, etc can be flattened or separate tables. 
    # For simplicity, we'll store some key aggregates or use JSON for flexible schema if DB supports it.
    # Since we are using generic SQL, let's define some core columns and maybe a json blob for the rest if needed.
    # However, user asked for "all football stats". 
    # To keep it manageable, let's map the most important ones or use a generic structure.
    # Given the breadth of FBref data, a JSON column for raw stats is often best to avoid 200+ columns.
    # But standard SQL often prefers columns. Let's put a few key ones and a 'data' blob for detailed stats.
    # Wait, SQLite supports JSON via extensions but maybe not by default in all python envs. 
    # Let's stick to a hybrid: key columns + flexible storage or just key columns for now.
    # Let's stick to a generic approach: Category (e.g. 'Defense'), and then the metric name/value? 
    # No, that makes querying hard. 
    # Let's create specific columns for the requested 'Defense' stats as an example, and 'Standard'.
    
    # Basic Stats
    goals_for = Column(Integer)
    goals_against = Column(Integer)
    possession = Column(Float)
    
    # Defense specific (example)
    tackles = Column(Integer)
    tackles_won = Column(Integer)
    blocks = Column(Integer)
    interceptions = Column(Integer)
    clearances = Column(Integer)
    
    team = relationship("Team", back_populates="squad_stats")
    
    __table_args__ = (UniqueConstraint('team_id', 'season', 'competition', name='_team_season_comp_uc'),)

class PlayerStat(Base):
    __tablename__ = 'player_stats'
    id = Column(Integer, primary_key=True)
    player_id = Column(Integer, ForeignKey('players.id'))
    season = Column(String, nullable=False)
    competition = Column(String, nullable=False)
    
    # Basic Stats
    minutes = Column(Integer)
    goals = Column(Integer)
    assists = Column(Integer)
    yellow_cards = Column(Integer)
    red_cards = Column(Integer)
    xg = Column(Float)
    npxg = Column(Float)
    xag = Column(Float)
    
    player = relationship("Player", back_populates="stats")
    
    __table_args__ = (UniqueConstraint('player_id', 'season', 'competition', name='_player_season_comp_uc'),)

def init_db(db_path='sqlite:///football_data.db'):
    engine = create_engine(db_path)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)

