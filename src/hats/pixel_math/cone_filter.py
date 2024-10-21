import astropy.units as u
from mocpy import MOC


def generate_cone_moc(ra: float, dec: float, radius_arcsec: float, order: int) -> MOC:
    """Generate a MOC object that covers the cone"""
    return MOC.from_cone(lon=ra * u.deg, lat=dec * u.deg, radius=radius_arcsec * u.arcsec, max_depth=order)
