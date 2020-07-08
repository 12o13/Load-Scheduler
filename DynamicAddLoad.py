# Household EMS battery, PV and HP
import datetime
import pandas as pd
import numpy as np
import math
from functools import reduce
import datapop as dpop

# reading load and generation ################################################
pathGenLoadSch = "C:/Users/robsi/source/repos/robert-thesis/ems-master/EMS_Simpson/DataSheets/load_schedule.xlsx"                    # Path for Load Schedule (DR)
pathGenLoadTime = "C:/Users/robsi/source/repos/robert-thesis/ems-master/EMS_Simpson/DataSheets/load_timing_april_week_3.xlsx"        # Path for Load in Calc (reading load consumption)
pathGenGenTime = "C:/Users/robsi/source/repos/robert-thesis/ems-master/EMS_Simpson/DataSheets/PV_BeforeMPPtracker_4kWp_Aachen_5min_FullYear.csv"               # Path for Generation data reading

#### Declaring Functions #####################################################


def LoadScan(LoadNum, GenTiming, LoadTiming, LoadSched):
    """
    Checks to see if load 'a' can be added to Power List properly, then does
    it if possible or tells you it cant. Currently LoadScan only works with
    the Load Number in Load Schedule and tells you when to add it.
        Parameters
    ----------
    LoadNum : int
        Load Number of Load in LoadSched.
    GenTiming : DataFrame
        DataFrame containing generation information over a day, Column-wise.
    LoadTiming : DataFrame
        DataFrame containing Load information over a day, Column-wise.
    LoadSched : DataFrame
        DataFrame containing the Load Schedule of all of the Loads in the system.
    Returns
    -------
    winningTime : int
        The time location of first possible run time
    """
    pTmp = GenTiming - LoadTiming
    pTmp = pTmp.rename(columns={0:"Net Power"}, errors="raise")
    pTmp['tmp'] = pTmp['Net Power'] - (LoadSched['Intermittant P'].loc[LoadNum,] \
                                       + LoadSched['Standby P'].loc[LoadNum,]) # Creates Temp Column to check against
    ser = pTmp[1] >= 0                                                         # Creates Temp Series to math against
    c = ser.expanding().apply(lambda r: reduce(lambda x, y: x + 1 if y
        else x * y, r))
    c[ser & (ser != ser.shift(-1))].value_counts()                             # Formats C to be checked against
    LoadL = (roundup(LoadSched.iloc[LoadNum].loc[['Length']]))/(5)
    WinnerAnnounced = 0
    for d in range(len(c)):
        if c[d] == (LoadL):                                                    # Looks for first Segment group which is positive (based on lod[#,])
            winingTime = d
            print('Can start at %s' %winingTime)                               # Prints out when to Start Load
            pTmp.drop(columns=['tmp'])
            WinnerAnnounced = 1
    if WinnerAnnounced == 0:
        print('Load cannot be added')
    pTmp.drop(columns=['tmp'])
    return winingTime

def TierLoads(LoadSchedule, LoadByLoad, LoadTiming):
    """
    Breaks out Tier Loads for the Data set based on the LoadTiming and LoadSchedule.

    Parameters
    ----------
    LoadSchedule : DataFrame
        DataFrame containing the Load Schedule of all of the Loads in the system.
    LoadByLoad : DataFrame
        LoadTiming with all of the loads seperated instead of sumed together.
    LoadTiming : DataFrame
        DataFrame containing Load information over a day, Column-wise.

    Returns
    -------
    T1LS : DataFrame
        Tier 1 Loads from the Load Schedule.
    T2LS : DataFrame
        Tier 2 Loads from the Load Schedule.
    T3LS : DataFrame
        Tier 3 Loads from the Load Schedule.
    T1Loads : DataFrame
        Load Timing DataFrame showing all of the Tier 1 Loads.
    T2Loads : DataFrame
        Load Timing DataFrame showing all of the Tier 2 Loads.
    T3Loads : DataFrame
        Load Timing DataFrame showing all of the Tier 3 Loads.

    """
    T1LS = LoadSchedule.query('Tier == 1')
    T2LS = LoadSchedule.query('Tier == 2')
    T3LS = LoadSchedule.query('Tier == 3')
    T1Loads = len(LoadTiming)*[None]
    T2Loads = len(LoadTiming)*[None]
    T3Loads = len(LoadTiming)*[None]
    T1Loads = pd.DataFrame(T1Loads)
    T2Loads = pd.DataFrame(T2Loads)
    T3Loads = pd.DataFrame(T3Loads)
    # Should have to TLoads with Column names as device
    for x in range(0,len(LoadSched.index)-1):
        if LoadSched.iloc[x,1] == 1:
            T1Loads[x] = pd.DataFrame(LoadByLoad.iloc[:,x])                    # add LoadByLoad.iloc(x) to T1Loads
        if LoadSched.iloc[x,1] == 2:
            T2Loads[x] = pd.DataFrame(LoadByLoad.iloc[:,x])
        if LoadSched.iloc[x,1] == 3:
            T3Loads[x] = pd.DataFrame(LoadByLoad.iloc[:,x])
    return T1LS, T2LS, T3LS, T1Loads, T2Loads, T3Loads

def roundup(x):
    return int(math.ceil(x / 5.0)) * 5

def IntLoad(Start, LoadNum, LoadSchedule, LoadByLoad, LoadTiming):
    """
    Adds an Intermittant Load to LoadByLoad at proper time interval

    Parameters
    ----------
    Start : int
        The time location to start the Load.
    LoadNum : int
        Load Number of Load in LoadSched.
    LoadSchedule : DataFrame
        DataFrame containing the Load Schedule of all of the Loads in the system.
    LoadByLoad : DataFrame
        LoadTiming with all of the loads seperated instead of sumed together.
    LoadTiming : DataFrame
        DataFrame containing Load information over a day, Column-wise.

    Returns
    -------
    LoadByLoad : DataFrame
        LoadTiming with all of the loads seperated instead of sumed together.
    LoadTiming : DataFrame
        DataFrame containing Load information over a day, Column-wise.

    """
# Need to update functions passed. Currently returns LoadByLoad automatically, but not LoadTiming!
    LoadL = (roundup(LoadSchedule.iloc[LoadNum].loc[['Length']]))/(5)
    LoadL = int(LoadL)
    for x in range(LoadL):
        LoadByLoad.loc[(Start+x), LoadNum] = LoadSched.iloc[LoadNum, 7]
    LoadTiming = LoadByLoad.sum(axis=1)
    return LoadByLoad, LoadTiming

def TOnLoad(Start, Length, LoadNum, LoadSchedule, LoadByLoad, LoadTiming):
    """
    Adds Load to LoadByLoad for proper Length

    Parameters
    ----------
    Start : int
        The time period to start the Load.
    Length : int
        Number of time periods.
    LoadNum : int
        Load Number of Load in LoadSched.
    LoadSchedule : DataFrame
        DataFrame containing the Load Schedule of all of the Loads in the system.
    LoadByLoad : DataFrame
        LoadTiming with all of the loads seperated instead of sumed together.
    LoadTiming : DataFrame
        DataFrame containing Load information over a day, Column-wise.

    Returns
    -------
    LoadByLoad : DataFrame
        LoadTiming with all of the loads seperated instead of sumed together.
    LoadTiming : DataFrame
        DataFrame containing Load information over a day, Column-wise.

    """
# Need to update functions passed. Currently returns LoadByLoad automatically, but not LoadTiming!
    for x in range(Length):
        LoadByLoad.loc[(Start+x), LoadNum] = LoadSched.iloc[LoadNum, 5]
    LoadTiming = LoadByLoad.sum(axis=1)
    return LoadByLoad, LoadTiming


def CreateLoadSched():
    """


    Parameters
    ----------
    LoadScheduleName : String
        The Name of the Load Schedule DataFrame.

    Returns
    -------
    LoadScheduleName : DataFrame
        An Empty Load Schedule.

    """
    headersLS = ['Load Name', 'Priority', 'Tier', 'Power Factor', 'Effeciency'
           , 'Voltage', 'Continuous P', 'Continuous Q', 'Intermittant P'
           , 'Intermittant Q', 'Standby P', 'Standby Q', 'Length', 'Start',
           'Notes'
           ]                                                                   # Set up DataFrame Headers
    LoadSchedule = pd.DataFrame(columns=headersLS)
    LoadSchedule = LoadSchedule.set_index('Load Name')
    return LoadSchedule

def AddLoadSched(LoadSchedule, LoadName, Priority, Tier, PowerFactor, Effeciency, Voltage
                 , ContinuousP, ContinuousQ, IntermittantP, IntermittantQ
                 , StandbyP, StandbyQ, Length, Start, Notes):
    """
!!! Does return updated LoadSchedule !!!

    Parameters
    ----------
    LoadSchedule : DataFrame
         DataFrame containing the Load Schedule of all of the Loads in the system.
    LoadName : string
        Name of the load to be added.
    Priority : int
        Priority for load to be modified incase of automated switching.
    Tier : int
        Load Tier: 1, 2, or 3.
    PowerFactor : float
        The Power Factor, between 0 and 1.
    Effeciency : int
        The loads electrical effeciency, between 0 and 1.
    Voltage : int
        Voltage the device operates on.
    ContinuousP : float
        Active Power used while load is active.
    ContinuousQ : float
        Reactive Power used while load is active.
    IntermittantP : float
        Active Power for a load that can be automatically switched.
    IntermittantQ : float
        Reactive Power for a load that can be automatically switched.
    StandbyP : float
        Active Power used while load is in Standby.
    StandbyQ : float
        Reactive Power used while load is in Standby.
    Length : int
        Number of time sequences the load must run.
    Start : int
        DataFrame the load is initiated.
    Notes : string
        Any notes about the load.

    Returns
    -------
    LoadSchedule : DataFrame
         DataFrame containing the Load Schedule of all of the Loads in the system.

    """
# Need to update functions passed. Currently does not automatically update the LoadSchedule !!
    new_row = {'Load Name': LoadName, 'Priority': Priority, 'Tier': Tier
          ,'Power Factor': PowerFactor, 'Effeciency': Effeciency
          , 'Voltage': Voltage, 'Continuous P': ContinuousP
          , 'Continuous Q': ContinuousQ, 'Intermittant P': IntermittantP
          , 'Intermittant Q': IntermittantQ, 'Standby P': StandbyP
          , 'Standby Q': StandbyQ, 'Length': Length, 'Start': Start
          , 'Notes': Notes
          }
    LoadSchedule = LoadSchedule.append(new_row, ignore_index=True)
    return LoadSchedule

