"""
collect.cnrfc.gages
============================================================
gages and watershed info for CNRFC forecast points
"""
# underscore aliases keep these names out of `from collect.cnrfc.gages import *`
from xml.etree import ElementTree
from collect.utils import utils


KLAMATH_GAGES = ['KEOO3L', 'BOYO3L', 'IRGC1L', 'BTYO3', 'SCNO3', 'CHSO3', 'WKAO3', 'WMSO3', 'KEOO3', 'BOYO3', 'IRGC1',
                 'YREC1', 'FTJC1', 'SEIC1', 'SBRC1', 'HAPC1', 'ONSC1', 'CEGC1', 'TRJC1', 'BURC1', 'HYMC1', 'HOOC1']

NORTHCOAST_GAGES = ['CREC1', 'FTDC1', 'BLKC1', 'ORIC1', 'MAUC1', 'ARCC1', 'BRGC1', 'DOSC1', 'PLBC1', 'FTSC1', 'LEGC1',
                    'MRNC1', 'SCOC1', 'FRNC1']

RUSSIANNAPA_GAGES = ['NVRC1', 'LAMC1', 'UKAC1', 'HOPC1', 'CDLC1', 'HEAC1', 'WSDC1', 'GUEC1', 'SHEC1', 'APCC1', 'HOPC1L',
                     'CDLC1L', 'HEAC1L', 'GUEC1L']

UPPERSACRAMENTO_GAGES = ['DLTC1', 'MMCC1', 'MSSC1', 'MSSC1F', 'CNBC1', 'PITC1F', 'SHDC1', 'PLYC1', 'CWAC1', 'COTC1',
                         'CWCC1', 'WHSC1', 'EDCC1', 'MLMC1', 'TCRC1', 'DCVC1', 'HKCC1', 'BKCC1', 'EPRC1', 'SGEC1',
                         'BLBC1', 'BDBC1', 'RDBC1', 'TEHC1', 'VWBC1', 'HAMC1', 'ORFC1', 'BTCC1', 'CLSC1', 'CLAC1',
                         'CLUC1', 'TISC1', 'WLKC1', 'RDBC1L', 'TEHC1L', 'BDBC1L', 'RDBC1L2', 'VWBC1L', 'ORFC1L']

FEATHERYUBA_GAGES = ['PLLC1', 'IIFC1', 'ANTC1', 'DVSC1', 'FHDC1', 'SCBC1', 'MFTC1', 'MRMC1', 'NFEC1', 'PLGC1', 'WBGC1',
                     'ORDC1', 'GYRC1', 'NBBC1', 'HLEC1', 'CFWC1', 'JKRC1', 'BWKC1', 'FOCC1', 'SUAC1', 'SOVC1', 'DMCC1',
                     'ROLC1I', 'DCSC1', 'JNSC1', 'OURC1', 'LOCC1', 'HCTC1', 'CFWC1O', 'DCWC1', 'NBBC1L', 'OURC1L',
                     'SUAC1L', 'JNSC1L', 'DCSC1L', 'LOCC1L', 'HLEC1L', 'MRYC1L', 'HLEC1LT', 'MRYC1LT', 'YUBC1L',
                     'OURC1R', 'JNSC1R', 'NBBC1R']

CACHEPUTAH_GAGES = ['MUPC1', 'KCVC1', 'SKPC1', 'CLKC1T', 'HOUC1', 'HCHC1', 'INVC1', 'PCGC1', 'LBEC1']

AMERICAN_GAGES = ['NFDC1', 'FMDC1', 'FMDC1O', 'BCTC1', 'LNLC1', 'RRGC1', 'HLLC1', 'HLLC1F', 'NMFC1', 'RUFC1', 'MFAC1',
                  'MFAC1F', 'UNVC1F', 'ICHC1', 'AKYC1', 'AKYC1F', 'CBAC1', 'CBAC1F', 'FOLC1', 'FOLC1F', 'UNVC1',
                  'SVCC1', 'MFAC1L', 'CBAC1L', 'RBBC1F', 'RBBC1SPL', 'LNLC1F', 'RRGC1L', 'RRGC1F', 'RUFC1L', 'SVCC1F',
                  'SVCC1L', 'HLLC1L', 'HLLC1SPL']

# Oct 2022 onwards
SALINASPAJARO_GAGES = []

# Oct 2022 onwards
SOUTHBAY_GAGES = []

LOWERSACRAMENTO_GAGES = ['SAMC1', 'SACC1', 'VONC1', 'FMWC1', 'DRMC1', 'RCVC1', 'FMWC1L', 'SACC1L', 'SAMC1L', 'NCOC1',
                         'NCOC1L']

# before Oct 2022
CENTRALCOAST_GAGES = ['LWDC1', 'SNRC1', 'NBYC1', 'NACC1', 'PRBC1', 'RDRC1', 'BSRC1', 'PIIC1', 'TESC1', 'HOSC1', 'PHOC1',
                      'AROC1', 'BTEC1', 'COYC1', 'ANDC1', 'CYTC1', 'CYEC1', 'CMIC1', 'LEXC1', 'ALRC1', 'GUAC1', 'CADC1',
                      'ANOC1', 'LVKC1', 'AHOC1', 'CVQC1', 'MPTC1', 'LRZC1', 'SFCC1', 'GUDC1', 'GSJC1']

SOUTHERNCALIFORNIA_GAGES = ['TWDC1', 'SSQC1', 'GARC1', 'LLYC1', 'CCHC1', 'SLUC1', 'VRVC1', 'CLLC1', 'EFBC1', 'CSKC1',
                            'PYMC1', 'LKPC1', 'YDRC1', 'SLOC1', 'TEKC1', 'VLKC1', 'MUTC1', 'SMHC1', 'SHRC1', 'TIMC1',
                            'SREC1', 'SRWC1', 'KNBC1', 'YTLC1', 'MWXC1', 'TSLC1', 'ADOC1', 'SVWC1', 'DKHC1', 'WFMC1',
                            'MVDC1O', 'MVVC1', 'HAWC1', 'ELPC1', 'SVIC1', 'FSNC1', 'CYBC1', 'HSAC1']

# Oct 2022 onwards
SANDIEGO_INLAND_GAGES = []

TULARE_GAGES = ['KKVC1', 'SKRC1', 'ISAC1', 'SCSC1', 'KTRC1', 'TMDC1', 'PFTC1', 'DLMC1', 'MLPC1']

SANJOAQUIN_GAGES = ['FRAC1', 'HIDC1', 'BHNC1', 'MPAC1', 'OWCC1', 'BNCC1', 'BCKC1', 'MEEC1', 'HPIC1', 'POHC1', 'EXQC1',
                    'HETC1', 'LNRC1', 'CHVC1F', 'NDPC1', 'NDVC1I', 'NSWC1I', 'AVYC1', 'NMSC1', 'LTDC1', 'DSNC1',
                    'DCMC1', 'STVC1', 'MDSC1', 'OBBC1', 'RIPC1', 'NWMC1', 'CRDC1', 'PATC1', 'VNSC1']

N_SANJOAQUIN_GAGES = ['CMPC1', 'NHGC1', 'MSGC1', 'FRGC1', 'EDOC1', 'SOSC1', 'MHBC1', 'MCNC1']

EASTSIERRA_GAGES = ['SUSC1', 'SCRN2', 'SCWN2', 'WOOC1', 'CEMC1', 'GRDN2', 'STWN2', 'FTCN2', 'WWBC1', 'PSRC1', 'STPC1',
                    'STPC1F', 'BCAC1', 'BCAC1F', 'FARC1', 'ILAC1', 'LWON2', 'TRRN2', 'VISN2', 'BPRC1', 'SGNC1', 'TAHC1',
                    'TRCC1', 'DNRC1', 'MTSC1']

HUMBOLDT_GAGES = ['MBON2', 'MHSN2', 'DVGN2', 'HREN2', 'DIXN2', 'HRCN2', 'PALN2', 'ROCN2', 'HBMN2', 'CMSN2', 'HRIN2',
                  'LHPN2', 'MARN2', 'MDCN2']

WATER_SUPPLY_INDICES = ['SACC0', 'VNSC0', 'MLIC0']

ALL = sorted({*KLAMATH_GAGES,
              *NORTHCOAST_GAGES,
              *RUSSIANNAPA_GAGES,
              *UPPERSACRAMENTO_GAGES,
              *FEATHERYUBA_GAGES,
              *CACHEPUTAH_GAGES,
              *AMERICAN_GAGES,
              *LOWERSACRAMENTO_GAGES,
              *CENTRALCOAST_GAGES,
              *SOUTHERNCALIFORNIA_GAGES,
              *TULARE_GAGES,
              *SANJOAQUIN_GAGES,
              *N_SANJOAQUIN_GAGES,
              *EASTSIERRA_GAGES,
              *HUMBOLDT_GAGES})

# river forecast points ("Forecast" and "Other"), served on the graphicalRVF routes
# source: https://www.cnrfc.noaa.gov/data/kml/riverFcst.xml (retrieved 2026-11-24; compare with get_gage_group)
RIVER_FORECAST = ['ACSC1', 'ADLC1', 'AHOC1', 'AKBC1', 'AKYC1', 'ALQC1', 'AMLC1', 'AMVC1', 'APCC1', 'ARCC1', 'AROC1',
                  'ATIC1', 'AVYC1', 'AWEC1', 'AYMC1', 'BDBC1', 'BKCC1', 'BLKC1', 'BOYO3', 'BRDC1', 'BRGC1', 'BRWC1',
                  'BSCC1', 'BSRC1', 'BTCC1', 'BTEC1', 'BTYO3', 'BURC1', 'CBAC1', 'CDLC1', 'CEMC1', 'CHBC1', 'CHLC1',
                  'CHSO3', 'CJKC1', 'CLAC1', 'CLKC1', 'CLLC1', 'CLSC1', 'CLUC1', 'CMIC1', 'CMSN2', 'CNBC1', 'COTC1',
                  'CRDC1', 'CREC1', 'CTIC1', 'CWAC1', 'CWCC1', 'CYBC1', 'CYEC1', 'CYTC1', 'DCMC1', 'DCSC1', 'DCVC1',
                  'DCWC1', 'DEPC1', 'DEVN2', 'DGYC1', 'DIXN2', 'DKCC1', 'DKHC1', 'DLMC1', 'DLTC1', 'DOSC1', 'DRIC1',
                  'DRMC1', 'DRYC1', 'DSNC1', 'EDCC1', 'EDOC1', 'ELNC1', 'ESRC1', 'ETTC1', 'EWSN2', 'FARC1', 'FBLC1',
                  'FMWC1', 'FPOC1', 'FRNC1', 'FSNC1', 'FTBC1', 'FTCN2', 'FTDC1', 'FTJC1', 'FTSC1', 'GARC1', 'GEYC1',
                  'GRCC1', 'GRDN2', 'GRIC1', 'GSJC1', 'GSRC1', 'GUDC1', 'GUEC1', 'GVAC1', 'GYRC1', 'HAMC1', 'HAPC1',
                  'HBMN2', 'HCHC1', 'HCTC1', 'HEAC1', 'HKCC1', 'HOOC1', 'HOPC1', 'HOSC1', 'HOUC1', 'HOYN2', 'HPIC1',
                  'HRCN2', 'HREN2', 'HRIN2', 'HYMC1', 'IIFC1', 'IRGC1', 'JAMC1', 'JNSC1', 'KCVC1', 'KCYC1', 'KEOO3',
                  'KGPC1', 'KKVC1', 'KLMC1', 'KRVC1', 'KTRC1', 'LCLN2', 'LEGC1', 'LGAC1', 'LGCC1', 'LHPN2', 'LLYC1',
                  'LOCC1', 'LRZC1', 'LSBC1', 'LSEC1', 'LTDC1', 'LWDC1', 'MARN2', 'MASN2', 'MAUC1', 'MBRC1', 'MCNC1',
                  'MDCN2', 'MDNC1', 'MDSC1', 'MEEC1', 'MFAC1', 'MFPC1', 'MFTC1', 'MHBC1', 'MHSN2', 'MLIC1', 'MLMC1',
                  'MLPC1', 'MMCC1', 'MOSC1', 'MPTC1', 'MRMC1', 'MRNC1', 'MRYC1', 'MSGC1', 'MSSC1', 'MTOC1', 'MUPC1',
                  'MUTC1', 'MVVC1', 'MWEC1', 'MWXC1', 'NBYC1', 'NCOC1', 'NFDC1', 'NFEC1', 'NHCC1', 'NILC1', 'NIXN2',
                  'NMFC1', 'NRWC1', 'NVRC1', 'NWMC1', 'OBBC1', 'ONSC1', 'OREC1', 'ORFC1', 'ORIC1', 'OTTC1', 'OURC1',
                  'PALN2', 'PATC1', 'PCGC1', 'PCNC1', 'PHOC1', 'PIIC1', 'PITC1', 'PLYC1', 'PNEC1', 'POHC1', 'PRBC1',
                  'PTTC1', 'RCVC1', 'RDBC1', 'RDGC1', 'RDRC1', 'RIOC1', 'RIPC1', 'RKLC1', 'RMKC1', 'RMSC1', 'ROCN2',
                  'RRIC1', 'RROC1', 'RUFC1', 'RVBC1', 'SACC1', 'SAMC1', 'SBRC1', 'SCBC1', 'SCNO3', 'SCOC1', 'SCPC1',
                  'SCRN2', 'SCWN2', 'SDDC1', 'SEIC1', 'SESC1', 'SFCC1', 'SFHN2', 'SGNC1', 'SHEC1', 'SJMC1', 'SKPC1',
                  'SKRC1', 'SLOC1', 'SLUC1', 'SMHC1', 'SOLC1', 'SOSC1', 'SPRC1', 'SPYC1', 'SREC1', 'SSAC1', 'SSQC1',
                  'SSTC1', 'STVC1', 'STWN2', 'SUSC1', 'TBDN2', 'TCCC1', 'TCRC1', 'TEHC1', 'TEKC1', 'TESC1', 'THLC1',
                  'THTC1', 'TIMC1', 'TISC1', 'TRCC1', 'TRJC1', 'TRNC1', 'TRRN2', 'TSLC1', 'TVRC1', 'UKAC1', 'UPCC1',
                  'VCAC1', 'VISN2', 'VNCC1', 'VNSC1', 'VONC1', 'VRVC1', 'VWBC1', 'WADN2', 'WALN2', 'WARC1', 'WBGC1',
                  'WFMC1', 'WKAO3', 'WLKC1', 'WMSO3', 'WOOC1', 'WPMC1', 'WPSC1', 'WTIC1', 'WWBC1', 'WWHN2', 'WWIC1',
                  'YDRC1', 'YLEC1', 'YLOC1', 'YREC1', 'YTLC1', 'YWPC1', 'ZNRC1']

# reservoir inflow forecast locations, served on the graphicalRVF-inflows routes
# source: https://www.cnrfc.noaa.gov/data/kml/rsvrInflow.xml (retrieved 2026-11-24; compare with get_gage_group)
RESERVOIR_INFLOW = ['ADOC1', 'ALRC1', 'ANDC1', 'ANOC1', 'ANTC1', 'BCAC1', 'BCKC1', 'BDBC1', 'BHNC1', 'BLBC1', 'BNCC1',
                    'BPRC1', 'BRTC1', 'BWKC1', 'CADC1', 'CBRC1', 'CCHC1', 'CEGC1', 'CFWC1', 'CHVC1', 'CKEC1', 'CLKC1',
                    'CMPC1', 'COYC1', 'CSKC1', 'CVQC1', 'DMCC1', 'DNRC1', 'DVAC1', 'DVSC1', 'EFBC1', 'ELPC1', 'EPRC1',
                    'EXQC1', 'FHDC1', 'FOCC1', 'FOLC1', 'FRAC1', 'FRGC1', 'GERO3', 'GUAC1', 'HETC1', 'HIDC1', 'HLEC1',
                    'HODC1', 'ICHC1', 'ILAC1', 'INVC1', 'ISAC1', 'JKRC1', 'KLAO3', 'KNFC1', 'KWKC1', 'LAMC1', 'LBDC1',
                    'LBEC1', 'LESC1', 'LEXC1', 'LKPC1', 'LNLC1', 'LNRC1', 'LWBC1', 'LWON2', 'MPAC1', 'MTSC1', 'MVDC1',
                    'NACC1', 'NBBC1', 'NDPC1', 'NDVC1', 'NHGC1', 'NIMC1', 'NMSC1', 'NSWC1', 'ORDC1', 'OURC1', 'OWCC1',
                    'PFTC1', 'PLBC1', 'PLLC1', 'PSRC1', 'PYMC1', 'ROLC1', 'SCSC1', 'SGEC1', 'SHDC1', 'SNRC1', 'SOVC1',
                    'STDC1', 'STPC1', 'SUAC1', 'SUDC1', 'SVIC1', 'SVKC1', 'SVWC1', 'TAHC1', 'TMDC1', 'TPZC1', 'TWDC1',
                    'UNVC1', 'UVRC1', 'VLKC1', 'WHSC1', 'WSDC1', 'ZSPC1']

# reservoir release forecast locations, served on the graphicalRelease routes
# source: https://www.cnrfc.noaa.gov/data/kml/rsvrRelease.xml (retrieved 2026-11-24; compare with get_gage_group)
RESERVOIR_RELEASE = ['ANDC1', 'BCVC1', 'BHNC1', 'BIFC1', 'BLBC1', 'BPRC1', 'CBRC1', 'CCHC1', 'CFWC1', 'CLKC1', 'CMCC1',
                     'COYC1', 'CSKC1', 'DNRC1', 'DVAC1', 'EFBC1', 'ELPC1', 'EPRC1', 'EXQC1', 'FKCC1', 'FRAC1', 'HAWC1',
                     'HETC1', 'HIDC1', 'HLEC1', 'ILAC1', 'INVC1', 'ISAC1', 'KNFC1', 'KRCC1', 'KWKC1', 'LAMC1', 'LBEC1',
                     'LEXC1', 'LKPC1', 'LLKC1', 'MACC1', 'MTSC1', 'MVDC1', 'NACC1', 'NBBC1', 'NDPC1', 'NHGC1', 'NIMC1',
                     'ORDC1', 'PFTC1', 'PORC1', 'PSRC1', 'SCSC1', 'SGEC1', 'SNRC1', 'STDC1', 'STPC1', 'SVIC1', 'SVKC1',
                     'TAHC1', 'TMDC1', 'TOPN2', 'UVRC1', 'WHSC1', 'WSDC1']

# ensemble forecast points
# source: https://www.cnrfc.noaa.gov/data/kml/ensPoints.xml (retrieved 2026-11-24; compare with get_gage_group)
ENSEMBLE_FORECAST = ['ADLC1', 'ADOC1', 'AHOC1', 'AKBC1', 'AKYC1', 'ALQC1', 'ALRC1', 'AMLC1', 'ANDC1', 'ANOC1', 'ANTC1',
                     'APCC1', 'ARCC1', 'AROC1', 'AVYC1', 'AWEC1', 'AYMC1', 'BCAC1', 'BCKC1', 'BDBC1', 'BHNC1', 'BKCC1',
                     'BLBC1', 'BNCC1', 'BOYO3', 'BPRC1', 'BRDC1', 'BRGC1', 'BRTC1', 'BSCC1', 'BSRC1', 'BTCC1', 'BTEC1',
                     'BTYO3', 'BURC1', 'BWKC1', 'CADC1', 'CBAC1', 'CBRC1', 'CCHC1', 'CDLC1', 'CEGC1', 'CEMC1', 'CFWC1',
                     'CHLC1', 'CHSO3', 'CHVC1', 'CKEC1', 'CLAC1', 'CLKC1', 'CLLC1', 'CLSC1', 'CLUC1', 'CMIC1', 'CMPC1',
                     'CMSN2', 'CNBC1', 'COTC1', 'COYC1', 'CREC1', 'CSKC1', 'CTIC1', 'CVQC1', 'CWAC1', 'CWCC1', 'CYBC1',
                     'DCMC1', 'DCSC1', 'DCVC1', 'DEPC1', 'DEVN2', 'DGYC1', 'DIXN2', 'DKCC1', 'DKHC1', 'DLMC1', 'DLTC1',
                     'DMCC1', 'DNRC1', 'DOSC1', 'DSNC1', 'DVAC1', 'DVSC1', 'EDCC1', 'EDOC1', 'EFBC1', 'ELPC1', 'EPRC1',
                     'ESRC1', 'EWSN2', 'EXQC1', 'FARC1', 'FHDC1', 'FMDC1', 'FMWC1', 'FOCC1', 'FOLC1', 'FPOC1', 'FRAC1',
                     'FRGC1', 'FRNC1', 'FSNC1', 'FTCN2', 'FTDC1', 'FTJC1', 'FTSC1', 'GARC1', 'GERO3', 'GEYC1', 'GRDN2',
                     'GUAC1', 'GUDC1', 'GUEC1', 'GYRC1', 'HAMC1', 'HAPC1', 'HAWC1', 'HBMN2', 'HCHC1', 'HEAC1', 'HETC1',
                     'HIDC1', 'HKCC1', 'HLEC1', 'HLLC1', 'HODC1', 'HOOC1', 'HOPC1', 'HOSC1', 'HOUC1', 'HOYN2', 'HPIC1',
                     'HRCN2', 'HREN2', 'HRIN2', 'HSAC1', 'HYMC1', 'ICHC1', 'IIFC1', 'ILAC1', 'INVC1', 'IRGC1', 'ISAC1',
                     'JKRC1', 'JNSC1', 'KCVC1', 'KCYC1', 'KEOO3', 'KGPC1', 'KKVC1', 'KLAO3', 'KLMC1', 'KNBC1', 'KRVC1',
                     'KTRC1', 'LAMC1', 'LBDC1', 'LBEC1', 'LCLN2', 'LEGC1', 'LESC1', 'LEXC1', 'LGAC1', 'LGCC1', 'LKPC1',
                     'LLYC1', 'LNLC1', 'LNRC1', 'LRZC1', 'LSEC1', 'LTDC1', 'LWBC1', 'LWDC1', 'LWON2', 'MARN2', 'MASN2',
                     'MBRC1', 'MCNC1', 'MDCN2', 'MDNC1', 'MDSC1', 'MEEC1', 'MFAC1', 'MFPC1', 'MFTC1', 'MHBC1', 'MHSN2',
                     'MLIC0', 'MLMC1', 'MLPC1', 'MMCC1', 'MPAC1', 'MPTC1', 'MRMC1', 'MRNC1', 'MRYC1', 'MSGC1', 'MSSC1',
                     'MTSC1', 'MUPC1', 'MUTC1', 'MVDC1', 'MVVC1', 'MWEC1', 'MWXC1', 'NACC1', 'NBBC1', 'NBYC1', 'NCOC1',
                     'NDPC1', 'NDVC1', 'NFDC1', 'NFEC1', 'NHCC1', 'NHGC1', 'NILC1', 'NMFC1', 'NMSC1', 'NRWC1', 'NSWC1',
                     'NVRC1', 'NWMC1', 'OBBC1', 'ONSC1', 'ORDC1', 'OREC1', 'ORFC1', 'ORIC1', 'OTTC1', 'OURC1', 'OWCC1',
                     'PALN2', 'PATC1', 'PCGC1', 'PCNC1', 'PFTC1', 'PHOC1', 'PIIC1', 'PITC1', 'PLBC1', 'PLLC1', 'PLYC1',
                     'PNEC1', 'POHC1', 'PRBC1', 'PSRC1', 'PTTC1', 'PYMC1', 'RBBC1', 'RDBC1', 'RDGC1', 'RDRC1', 'RIOC1',
                     'RIPC1', 'RKLC1', 'RMKC1', 'RMSC1', 'ROCN2', 'ROLC1', 'RROC1', 'RUFC1', 'SACC0', 'SACC1', 'SAMC1',
                     'SBRC1', 'SCBC1', 'SCNO3', 'SCOC1', 'SCPC1', 'SCRN2', 'SCSC1', 'SCWN2', 'SDDC1', 'SEIC1', 'SESC1',
                     'SFCC1', 'SGEC1', 'SGNC1', 'SHDC1', 'SHEC1', 'SHRC1', 'SKPC1', 'SKRC1', 'SLOC1', 'SLUC1', 'SMHC1',
                     'SNRC1', 'SOLC1', 'SOSC1', 'SOVC1', 'SPRC1', 'SPYC1', 'SREC1', 'SSAC1', 'SSQC1', 'STDC1', 'STPC1',
                     'STVC1', 'STWN2', 'SUAC1', 'SUDC1', 'SUSC1', 'SVCC1', 'SVIC1', 'SVKC1', 'SVWC1', 'TAHC1', 'TBDN2',
                     'TCCC1', 'TCRC1', 'TEHC1', 'TEKC1', 'TESC1', 'THLC1', 'TIMC1', 'TISC1', 'TMDC1', 'TOPN2', 'TRCC1',
                     'TRJC1', 'TRNC1', 'TRRN2', 'TSLC1', 'TVRC1', 'TWDC1', 'UKAC1', 'UNVC1', 'UPCC1', 'UVRC1', 'VCAC1',
                     'VISN2', 'VLKC1', 'VNSC0', 'VNSC1', 'VONC1', 'VRVC1', 'VWBC1', 'WADN2', 'WARC1', 'WBGC1', 'WFMC1',
                     'WHSC1', 'WKAO3', 'WLKC1', 'WMSO3', 'WOOC1', 'WPMC1', 'WPSC1', 'WSDC1', 'WTIC1', 'WWBC1', 'WWHN2',
                     'WWIC1', 'YDRC1', 'YLEC1', 'YLOC1', 'YREC1', 'YTLC1', 'YWPC1', 'ZSPC1']


def get_gage_group(rvf=True, inflow=False, release=False, ensemble=False):
    """
    current CNRFC forecast location codes for the selected forecast products, read from the location lists behind
    the CNRFC home page map:

    https://www.cnrfc.noaa.gov/data/kml/riverFcst.xml      river forecast points (rvf)
    https://www.cnrfc.noaa.gov/data/kml/rsvrInflow.xml     reservoir inflow forecast points (inflow)
    https://www.cnrfc.noaa.gov/data/kml/rsvrRelease.xml    reservoir release forecast points (release)
    https://www.cnrfc.noaa.gov/data/kml/ensPoints.xml      ensemble forecast points (ensemble)

    Arguments:
        rvf (bool): include river forecast points, the "Forecast" and "Other" points served on graphicalRVF routes
        inflow (bool): include reservoir inflow forecast points, served on graphicalRVF-inflows routes
        release (bool): include reservoir release forecast points, served on graphicalRelease routes
        ensemble (bool): include ensemble forecast points
    Returns:
        (list): sorted, unique location codes in any of the selected groups; empty if no group is selected
    """
    groups = {'riverFcst': rvf, 'rsvrInflow': inflow, 'rsvrRelease': release, 'ensPoints': ensemble}
    gages = set()
    for name, selected in groups.items():
        if selected:
            response = utils.get_session_response(f'https://www.cnrfc.noaa.gov/data/kml/{name}.xml')
            response.raise_for_status()

            # each location is an element named for its list, e.g. <rsvrRelease id="ORDC1" ... />
            gages.update(element.get('id') for element in ElementTree.fromstring(response.content).iter(name))
    return sorted(gages)
