#!/bin/usr/python

import mysql.connector as sqlcon
import pandas as pd
import pandas.io.sql as sql
import numpy as np
import datetime as dt
import sys
import os
import time

root_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + os.path.sep
sys.path.append(root_path + 'config')
import db_config_hl as db
sys.path.append(root_path + 'utility_scripts')
from utils import make_dir

FIRST_HOUR = 11
LAST_HOUR = 22
input_path = root_path + 'roster_automation/inputs/'
output_path = root_path + 'roster_automation/outputs/'
make_dir(input_path)
make_dir(output_path)


def main():
    monday = dt.date(2016, 8, 8)
    clusters = get_clusters()
    print len(clusters), 'clusters found'
    rider_data = pd.read_csv(input_path + 'riders_' + monday.strftime('%Y%m%d') + '.csv')
    rider_data.fillna(0, inplace=True)
    final_stats = None
    final_ideal_mix = None
    final_report = None
    # for cluster in ['BTM']:
    for cluster in clusters.keys():
        print cluster
        sub_clusters = clusters[cluster]
        ideal_mix, cluster_report = get_roster(cluster, sub_clusters, monday, rider_data)
        final_ideal_mix = pd.concat([final_ideal_mix, ideal_mix], ignore_index=True)
        final_report = pd.concat([final_report, cluster_report], ignore_index=True)
    #     stats = get_stats(roster)
    #     final_stats = pd.concat([final_stats, stats], ignore_index=True)
    # final_stats.to_csv('output/' + 'roster_stats_ist_' + monday.strftime('%Y%m%d') + '.csv',
    #                    index=False, encoding='utf-8')
    final_ideal_mix.to_csv(output_path + 'ideal_rider_mix_' + monday.strftime('%Y%m%d') + '.csv',
                           index=False, encoding='utf-8')
    final_report.to_csv(output_path + 'roster_report_' + monday.strftime('%Y%m%d') + '.csv',
                        index=False, encoding='utf-8')


# def get_clusters():
#     clusters = {
#         # 'BTM': {
#         #     'S1': [625, 2146, 2614, 2682],
#         #     'S2': [1379],
#         #     'S3': [2692],
#         #     'S4': [2207, 2326],
#         #     'Others': [2208, 2685, 2749, 1426, 2604, 2409, 2747, 2381]
#         # },
#         'BTM': {
#             'S1': [2860],
#             'S2': [1379],
#             'S3': [2207],
#             # 'S4': [1805, 2208],
#             'S5': [2146, 2682, 625],
#             'S6': [652, 1535, 1968, 2786, 1834],
#             'Others': []
#         },
#         'Domlur': {
#             'S1': [1972, 343, 2606],
#             'S2': [311, 372, 2573],
#             'Others': []
#         }
#     }
#     return clusters


def get_clusters():
    cluster_data = pd.read_csv(input_path + 'subcluster_seller_mapping.csv')
    cluster_data = cluster_data.groupby(['city', 'cluster_name', 'sub_cluster'])
    clusters = {}
    for [city, cluster_name, sub_cluster], mapping in cluster_data:
        if clusters.get(cluster_name) == None:
            clusters[cluster_name] = {'Others': []}
        clusters[cluster_name][sub_cluster] = mapping.seller_id.tolist()
    return clusters


def get_roster(cluster, sub_clusters, monday, rider_data):
    schedule_4h = None
    schedule_constrained = None
    roster_4h = None
    roster_constrained = None
    ideal_mix = pd.DataFrame([], columns=['cluster', 'sub_cluster', 'date', '5D', 'FT', 'BS', 'PT'])
    cluster_report = None
    for d in xrange(7):
        date = monday + dt.timedelta(d)
        print date
        riders = rider_data
        # riders = get_cluster_riders_available(rider_data, cluster, date)
        for sub_cluster in sub_clusters.keys():
            if sub_cluster == 'Others':
                continue
            print sub_cluster
            required = get_demand(date, sub_clusters[sub_cluster])
            slots_4h, required_4h, rider_cnt = get_ideal_schedule(
                required.copy(), cluster, date, sub_cluster)
            slots_constrained, required_constrained = get_constrained_schedule(required.copy(),
                                                                               riders, cluster,
                                                                               date, sub_cluster)
            # slots_constrained, required_constrained, riders = get_schedule(required, riders,
            # cluster, date, sub_cluster)
            report = get_report(
                required_4h.copy(), required_constrained.copy(), cluster, sub_cluster, date)
            schedule_4h = pd.concat([schedule_4h, slots_4h], ignore_index=True)
            schedule_constrained = pd.concat(
                [schedule_constrained, slots_constrained], ignore_index=True)
            roster_4h = pd.concat([roster_4h, required_4h], ignore_index=True)
            roster_constrained = pd.concat(
                [roster_constrained, required_constrained], ignore_index=True)
            ideal_mix = ideal_mix.append(pd.Series([cluster, sub_cluster, date] + list(rider_cnt),
                                                   index=['cluster', 'sub_cluster', 'date',
                                                          '5D', 'FT', 'BS', 'PT']),
                                         ignore_index=True)
            cluster_report = pd.concat([cluster_report, report], ignore_index=True)

    schedule_4h = schedule_4h[['cluster', 'sub_cluster', 'date', 'rider',
                               'start_time', 'end_time', 'duration']]
    schedule_constrained = schedule_constrained[['cluster', 'sub_cluster', 'date', 'rider',
                                                 'start_time', 'end_time', 'duration']]
    roster_4h = roster_4h[['cluster', 'sub_cluster', 'date', 'hour', 'orders',
                           'ideal_rider_count', 'actual_rider_count', 'shortage']]
    roster_constrained = roster_constrained[['cluster', 'sub_cluster', 'date', 'hour', 'orders',
                                             'ideal_rider_count', 'actual_rider_count', 'shortage']]

    write_data_to_files(
        schedule_4h, roster_4h, schedule_constrained, roster_constrained, cluster, monday)

    return ideal_mix, cluster_report


def get_demand(date, sellers):
    required = pd.DataFrame([[0]] * 12, index=range(FIRST_HOUR, LAST_HOUR + 1), columns=['orders'])
    orders = get_orders(date, sellers)
    ind = orders.hour.isin([12, 13, 20, 21])
    orders.loc[ind, 'orders'] *= 0.75
    orders = orders.pivot_table(index='hour', values='orders', aggfunc=np.mean)
    required['orders'] = orders
    required.fillna(0, inplace=True)
    required['ideal_rider_count'] = np.ceil(required.orders / 1.5)
    required['actual_rider_count'] = 0
    required['shortage'] = required.ideal_rider_count - required.actual_rider_count
    return required


def get_orders(date, sellers):
    date_list = [str(date - dt.timedelta(7)), str(date - dt.timedelta(14)),
                 str(date - dt.timedelta(21)), str(date - dt.timedelta(28))]
    query = "select date(o.scheduled_time) date, \
        hour(convert_tz(o.scheduled_time,'UTC','Asia/Kolkata')) hour, count(o.id) \
        orders from coreengine_order o where o.status!=302 and o.seller_id in (" + \
            ','.join([str(x) for x in sellers]) + ") and date(scheduled_time) in ('" + \
            "','".join(date_list) + "') group by 1,2 having hour between " + str(FIRST_HOUR) \
            + " and " + str(LAST_HOUR) + ";"
    return sql.read_sql(query, cnx)


def get_ideal_schedule(required, cluster, date, sub_cluster, shift=4):
    slots, required = get_slots(required, shift)
    slots, rider_cnt = assign_riders(slots)
    slots, required = add_cluster(slots, required, cluster, sub_cluster, date)
    return slots, required, rider_cnt


def get_slots(required, shift):
    slots = pd.DataFrame([], columns=['start_time', 'end_time', 'duration'])
    for ind in required.index:
        if required.loc[ind, 'shortage'] > 0:
            shortage = int(required.loc[ind, 'shortage'])
            if ind > (LAST_HOUR - shift + 1):
                start_time = LAST_HOUR - shift + 1
                end_time = LAST_HOUR + 1
            else:
                start_time = ind
                end_time = ind + shift

            slots = slots.append(pd.DataFrame([[start_time, end_time, shift]] * shortage,
                                              columns=['start_time', 'end_time', 'duration']),
                                 ignore_index=True)
            required.loc[start_time: end_time - 1, 'actual_rider_count'] += shortage
            required.loc[start_time: end_time - 1, 'shortage'] -= shortage
    return slots, required


def assign_riders(slots):
    slots['rider'] = None
    unassigned_slots = slots.rider.isnull()
    riders_assigned_cnt_5d = 0
    riders_assigned_cnt_ft = 0
    riders_assigned_cnt_bs = 0
    riders_assigned_cnt_pt = 0
    while unassigned_slots.sum():
        start_time = slots.loc[unassigned_slots, 'start_time'].min()
        if start_time < (LAST_HOUR - 10):
            riders_5d = min((unassigned_slots & (slots.start_time == start_time)).sum(),
                            (unassigned_slots & (slots.start_time == (start_time + 4))).sum(),
                            (unassigned_slots & (slots.start_time == (start_time + 8))).sum())
            if riders_5d:
                # print 'assigning 5D', riders_5d
                rider_id_5d = get_rider_id('5D', riders_assigned_cnt_5d, riders_5d)
                ind = slots.index[unassigned_slots & (slots.start_time == start_time)][:riders_5d]
                slots.loc[ind, 'rider'] = rider_id_5d
                ind = slots.index[
                    unassigned_slots & (slots.start_time == (start_time + 4))][:riders_5d]
                slots.loc[ind, 'rider'] = rider_id_5d
                ind = slots.index[
                    unassigned_slots & (slots.start_time == (start_time + 8))][:riders_5d]
                slots.loc[ind, 'rider'] = rider_id_5d
                unassigned_slots = slots.rider.isnull()
                riders_assigned_cnt_5d += riders_5d

        if (unassigned_slots.sum() and
            (start_time == slots.loc[unassigned_slots, 'start_time'].min()) and
                (start_time < (LAST_HOUR - 7))):
            slot1_cnt = (unassigned_slots & (slots.start_time == start_time)).sum()
            ind = (unassigned_slots & (slots.start_time > (start_time + 4)))
            if ind.sum():
                slot2_hours = slots.loc[ind].pivot_table(
                    index='start_time', values='end_time', aggfunc=len)
                slot2_start_time, slot2_cnt = slot2_hours.idxmax(), slot2_hours.max()
            else:
                slot2_cnt = 0
            riders_bs = min(slot1_cnt, slot2_cnt)
            if riders_bs:
                # print 'assigning BS', riders_bs
                rider_id_bs = get_rider_id('BS', riders_assigned_cnt_bs, riders_bs)
                ind = slots.index[unassigned_slots & (slots.start_time == start_time)][:riders_bs]
                slots.loc[ind, 'rider'] = rider_id_bs
                ind = slots.index[
                    unassigned_slots & (slots.start_time == slot2_start_time)][:riders_bs]
                slots.loc[ind, 'rider'] = rider_id_bs
                unassigned_slots = slots.rider.isnull()
                riders_assigned_cnt_bs += riders_bs

        if (unassigned_slots.sum() and
            (start_time == slots.loc[unassigned_slots, 'start_time'].min()) and
                (start_time < (LAST_HOUR - 6))):
            riders_ft = min((unassigned_slots & (slots.start_time == start_time)).sum(),
                            (unassigned_slots & (slots.start_time == (start_time + 4))).sum())
            if riders_ft:
                rider_id_ft = get_rider_id('FT', riders_assigned_cnt_ft, riders_ft)
                ind = slots.index[unassigned_slots & (slots.start_time == start_time)][:riders_ft]
                slots.loc[ind, 'rider'] = rider_id_ft
                ind = slots.index[
                    unassigned_slots & (slots.start_time == (start_time + 4))][:riders_ft]
                slots.loc[ind, 'rider'] = rider_id_ft
                unassigned_slots = slots.rider.isnull()
                riders_assigned_cnt_ft += riders_ft

        if (unassigned_slots.sum() and
                (start_time == slots.loc[unassigned_slots, 'start_time'].min())):
            riders_pt = (unassigned_slots & (slots.start_time == start_time)).sum()
            if riders_pt:
                rider_id_pt = get_rider_id('PT', riders_assigned_cnt_pt, riders_pt)
                ind = slots.index[unassigned_slots & (slots.start_time == start_time)][:riders_pt]
                slots.loc[ind, 'rider'] = rider_id_pt
                unassigned_slots = slots.rider.isnull()
                riders_assigned_cnt_pt += riders_pt
    return slots, (riders_assigned_cnt_5d, riders_assigned_cnt_ft,
                   riders_assigned_cnt_bs, riders_assigned_cnt_pt)


def get_constrained_schedule(required, riders, cluster, date, sub_cluster):
    slots = pd.DataFrame([], columns=['start_time', 'end_time', 'duration', 'rider'])
    riders_available_5d, riders_available_ft, riders_available_bs, riders_available_pt \
        = get_sub_cluster_riders_available(riders, cluster, sub_cluster, date)
    # riders_available_5d, riders_available_ft, riders_available_bs, riders_available_pt = riders
    print riders_available_5d, riders_available_ft, riders_available_bs, riders_available_pt

    riders_assigned_cnt_5d = 0
    riders_assigned_cnt_ft = 0
    riders_assigned_cnt_bs = 0
    riders_assigned_cnt_pt = 0

    mode = required.shortage.mode()
    if len(mode):
        mode = mode[0]
    else:
        mode = required.shortage.mean()
    riders_5d = int(min(riders_available_5d, mode))
    if riders_5d > 0:
        # print 'assigning 5D', riders_5d
        rider_id_5d = get_rider_id('5D', riders_assigned_cnt_5d, riders_5d)
        start_ind = len(slots)
        slots = slots.append(pd.DataFrame([[11, 23, 12]] * riders_5d,
                                          columns=['start_time', 'end_time', 'duration']),
                             ignore_index=True)
        slots.loc[start_ind:, 'rider'] = rider_id_5d
        required.loc[11:22, 'actual_rider_count'] += riders_5d
        required.loc[11:22, 'shortage'] -= riders_5d
        riders_assigned_cnt_5d += riders_5d
        riders_available_5d -= riders_5d

    for ind in required.index:
        shortage = required.loc[ind, 'shortage']
        if (shortage > 0) and (ind < 15):
            slot1_cnt = shortage
            slot2_cnt = required.loc[ind + 4:, 'shortage'].max()
            if (required.shortage == slot2_cnt).sum() > 1:
                slot2_start_time = required.loc[ind + 5:, 'shortage'].idxmax()
            else:
                slot2_start_time = required.loc[ind + 4:, 'shortage'].idxmax()
            riders_bs = int(min(slot1_cnt, slot2_cnt, riders_available_bs))
            if (riders_bs > 0) and ((slot2_start_time != ind + 4)):
                # print 'assigning BS', riders_bs
                rider_id_bs = get_rider_id('BS', riders_assigned_cnt_bs, riders_bs)
                start_ind = len(slots)
                slots = slots.append(pd.DataFrame([[ind, ind + 4, 4]] * riders_bs,
                                                  columns=['start_time', 'end_time', 'duration']),
                                     ignore_index=True)
                slots.loc[start_ind:, 'rider'] = rider_id_bs
                required.loc[ind:ind + 3, 'actual_rider_count'] += riders_bs
                required.loc[ind:ind + 3, 'shortage'] -= riders_bs

                if slot2_start_time > 19:
                    slot2_start_time = 19
                start_ind = len(slots)
                slots = slots.append(pd.DataFrame([[slot2_start_time, slot2_start_time + 4,
                                                    4]] * riders_bs,
                                                  columns=['start_time', 'end_time', 'duration']),
                                     ignore_index=True)
                slots.loc[start_ind:, 'rider'] = rider_id_bs
                required.loc[
                    slot2_start_time:slot2_start_time + 3, 'actual_rider_count'] += riders_bs
                required.loc[slot2_start_time:slot2_start_time + 3, 'shortage'] -= riders_bs

                riders_assigned_cnt_bs += riders_bs
                riders_available_bs -= riders_bs
                shortage = required.loc[ind, 'shortage']

        if (shortage > 0) and (ind < 16):
            mode = required.loc[ind: ind + 7, 'shortage'].mode()
            if len(mode):
                mode = mode[0]
            else:
                mode = required.shortage.mean()
            riders_ft = int(min(riders_available_ft, mode))
            if riders_ft > 0:
                # print 'assigning FT', riders_ft
                rider_id_ft = get_rider_id('FT', riders_assigned_cnt_ft, riders_ft)
                start_ind = len(slots)
                slots = slots.append(pd.DataFrame([[ind, ind + 8, 8]] * riders_ft,
                                                  columns=['start_time', 'end_time', 'duration']),
                                     ignore_index=True)
                slots.loc[start_ind:, 'rider'] = rider_id_ft
                required.loc[ind: ind + 7, 'actual_rider_count'] += riders_ft
                required.loc[ind: ind + 7, 'shortage'] -= riders_ft
                riders_assigned_cnt_ft += riders_ft
                riders_available_ft -= riders_ft
                shortage = required.loc[ind, 'shortage']

        if shortage > 0:
            riders_pt = int(min(riders_available_pt, shortage))
            if riders_pt > 0:
                # print 'assigning PT', riders_pt
                rider_id_pt = get_rider_id('PT', riders_assigned_cnt_pt, riders_pt)
                start_ind = len(slots)
                if ind > 19:
                    start_time = 19
                else:
                    start_time = ind
                slots = slots.append(pd.DataFrame([[start_time, start_time + 4, 4]] * riders_pt,
                                                  columns=['start_time', 'end_time', 'duration']),
                                     ignore_index=True)
                slots.loc[start_ind:, 'rider'] = rider_id_pt
                required.loc[start_time: start_time + 3, 'actual_rider_count'] += riders_pt
                required.loc[start_time: start_time + 3, 'shortage'] -= riders_pt
                riders_assigned_cnt_pt += riders_pt
                riders_available_pt -= riders_pt
                shortage = required.loc[ind, 'shortage']

        if shortage > 0:
            riders_ft = int(min(riders_available_ft, shortage))
            if riders_ft > 0:
                # print 'assigning FT', riders_ft
                rider_id_ft = get_rider_id('FT', riders_assigned_cnt_ft, riders_ft)
                start_ind = len(slots)
                if ind > 15:
                    start_time = 15
                else:
                    start_time = ind
                slots = slots.append(pd.DataFrame([[start_time, start_time + 8, 8]] * riders_ft,
                                                  columns=['start_time', 'end_time', 'duration']),
                                     ignore_index=True)
                slots.loc[start_ind:, 'rider'] = rider_id_ft
                required.loc[start_time: start_time + 7, 'actual_rider_count'] += riders_ft
                required.loc[start_time: start_time + 7, 'shortage'] -= riders_ft
                riders_assigned_cnt_ft += riders_ft
                riders_available_ft -= riders_ft
                pt_ind = slots.index[slots.rider.apply(lambda x: x[:2]).isin(['PT'])]
                for pt_i in pt_ind:
                    start_time = int(slots.loc[pt_i, 'start_time'])
                    if required.loc[start_time:start_time + 3, 'shortage'].max() < 0:
                        slots.drop(pt_i, inplace=True)
                        required.loc[start_time:start_time + 3, 'actual_rider_count'] -= 1
                        required.loc[start_time:start_time + 3, 'shortage'] += 1
                        riders_available_pt += 1
                shortage = required.loc[ind, 'shortage']

        if shortage > 0:
            riders_5d = int(min(riders_available_5d, shortage))
            if riders_5d > 0:
                # print 'assigning 5D', riders_5d
                rider_id_5d = get_rider_id('5D', riders_assigned_cnt_5d, riders_5d)
                start_ind = len(slots)
                slots = slots.append(pd.DataFrame([[11, 23, 12]] * riders_5d,
                                                  columns=['start_time', 'end_time', 'duration']),
                                     ignore_index=True)
                slots.loc[start_ind:, 'rider'] = rider_id_5d
                required.loc[11:22, 'actual_rider_count'] += riders_5d
                required.loc[11:22, 'shortage'] -= riders_5d
                riders_assigned_cnt_5d += riders_5d
                riders_available_5d -= riders_5d
                ft_ind = slots.index[slots.rider.apply(lambda x: x[:2]).isin(['FT'])]
                for ft_i in ft_ind:
                    start_time = int(slots.loc[ft_i, 'start_time'])
                    if required.loc[start_time:start_time + 7, 'shortage'].max() < 0:
                        slots.drop(ft_i, inplace=True)
                        required.loc[start_time:start_time + 7, 'actual_rider_count'] -= 1
                        required.loc[start_time:start_time + 7, 'shortage'] += 1
                        riders_available_ft += 1
                pt_ind = slots.index[slots.rider.apply(lambda x: x[:2]).isin(['PT'])]
                for pt_i in pt_ind:
                    start_time = int(slots.loc[pt_i, 'start_time'])
                    if required.loc[start_time:start_time + 3, 'shortage'].max() < 0:
                        slots.drop(pt_i, inplace=True)
                        required.loc[start_time:start_time + 3, 'actual_rider_count'] -= 1
                        required.loc[start_time:start_time + 3, 'shortage'] += 1
                        riders_available_pt += 1
                shortage = required.loc[ind, 'shortage']

        if shortage > 0:
            print "Can't build roster for", cluster, date, sub_cluster, ind

    slots, required = add_cluster(slots, required, cluster, sub_cluster, date)
    # return slots, required, [riders_available_5d, riders_available_ft,
    #                          riders_available_bs, riders_available_pt]
    return slots, required


def add_cluster(slots, required, cluster, sub_cluster, date):
    slots['cluster'] = cluster
    slots['sub_cluster'] = sub_cluster
    slots['date'] = date
    # slots = slots[['cluster', 'sub_cluster', 'date', 'rider_id',
    #                'start_time', 'end_time', 'duration']]
    required['cluster'] = cluster
    required['sub_cluster'] = sub_cluster
    required['date'] = date
    required['hour'] = required.index
    # required = required[['cluster', 'sub_cluster', 'date', 'hour', 'orders',
    #                      'ideal_rider_count', 'actual_rider_count', 'shortage']]
    return slots, required


def get_cluster_riders_available(rider_data, cluster, date):
    riders_available_5d = int(riders.loc[(rider_data.weekday_num == date.weekday()) &
                                         (rider_data.cluster == cluster), '5d'].iloc[0])
    riders_available_ft = int(riders.loc[(rider_data.weekday_num == date.weekday()) &
                                         (rider_data.cluster == cluster), 'ft'].iloc[0])
    riders_available_bs = int(riders.loc[(rider_data.weekday_num == date.weekday()) &
                                         (rider_data.cluster == cluster), 'bs'].iloc[0])
    riders_available_pt = int(riders.loc[(rider_data.weekday_num == date.weekday()) &
                                         (rider_data.cluster == cluster), 'pt'].iloc[0])
    return riders_available_5d, riders_available_ft, riders_available_bs, riders_available_pt


def get_sub_cluster_riders_available(rider_data, cluster, sub_cluster, date):
    riders_available_5d = int(rider_data.loc[(rider_data.weekday_num == date.weekday()) &
                                             (rider_data.cluster == cluster) &
                                             (rider_data.sub_cluster == sub_cluster), '5d'].iloc[0])
    riders_available_ft = int(rider_data.loc[(rider_data.weekday_num == date.weekday()) &
                                             (rider_data.cluster == cluster) &
                                             (rider_data.sub_cluster == sub_cluster), 'ft'].iloc[0])
    riders_available_bs = int(rider_data.loc[(rider_data.weekday_num == date.weekday()) &
                                             (rider_data.cluster == cluster) &
                                             (rider_data.sub_cluster == sub_cluster), 'bs'].iloc[0])
    riders_available_pt = int(rider_data.loc[(rider_data.weekday_num == date.weekday()) &
                                             (rider_data.cluster == cluster) &
                                             (rider_data.sub_cluster == sub_cluster), 'pt'].iloc[0])
    return riders_available_5d, riders_available_ft, riders_available_bs, riders_available_pt


# def get_report(schedule_4h, schedule):
#     report = pd.DataFrame([], columns=['cluster', 'sub_cluster', 'date', 'metric'] +
#                                       range(11, 23))
#     schedule_4h_grouped = schedule_4h.groupby(['cluster', 'sub_cluster', 'date'])
#     for (cluster, sub_cluster, date), day_schedule_4h in schedule_4h_grouped:
#         row = {'cluster': cluster, 'sub_cluster': sub_cluster,
#                'date': date, 'metric': 'ideal_slots'}
#         slots_4h = day_schedule_4h.pivot_table(index='start_time', values='duration', aggfunc=len)
#         slots_4h = slots_4h.append(pd.Series(row))
#         # print slots_4h
#         report = report.append(slots_4h, ignore_index=True)
#
#     schedule_grouped = schedule.groupby(['cluster', 'sub_cluster', 'date'])
#     for (cluster, sub_cluster, date), day_schedule in schedule_grouped:
#         row = {'cluster': cluster, 'sub_cluster': sub_cluster,
#                'date': date, 'metric': 'planned_slots'}
#         for ind in day_schedule.index:
#             hour = day_schedule.loc[ind, 'start_time']
#             row[hour] = row.get(hour, 0) + 1
#             if day_schedule.loc[ind, 'duration'] > 4:
#                 row[hour + 4] = row.get(hour + 4, 0) + 1
#             if day_schedule.loc[ind, 'duration'] > 8:
#                 row[hour + 8] = row.get(hour + 8, 0) + 1
#         slots_4h = pd.Series(row)
#         # print slots_4h
#         report = report.append(slots_4h, ignore_index=True)
#     # print report
#     # exit(-1)
#     report.fillna(0, inplace=True)
#     report.sort_values(['cluster', 'sub_cluster', 'date', 'metric'], inplace=True)
#     return report

def get_report(required_4h, required_constrained, cluster, sub_cluster, date):
    report = required_4h[['ideal_rider_count', 'actual_rider_count']].transpose()
    report = report.append(required_constrained['actual_rider_count'])
    report = report.append(pd.DataFrame([[]]*2), ignore_index=True)
    report['cluster'] = cluster
    report['sub_cluster'] = sub_cluster
    report['date'] = date
    report['metric'] = ['Q0', 'Q1', 'Q2', 'Q3', 'Q4']
    report = report[['cluster', 'sub_cluster', 'date', 'metric'] + range(11, 23)]
    report['total'] = report[range(11, 23)].sum(1)
    return report


def get_stats(roster):
    stats = roster.pivot_table(index=['cluster', 'sub_cluster', 'date'], values=[
        'ideal_rider_count', 'actual_rider_count'], aggfunc=sum)
    stats.columns = ['actual_man_hours', 'ideal_man_hours']
    stats.reset_index(inplace=True)
    stats['excess'] = stats.actual_man_hours - stats.ideal_man_hours
    stats['excess_perc'] = stats.excess / stats.ideal_man_hours.apply(lambda x: max(x, 1))
    return stats


def get_rider_id(prefix, assigned_count, count):
    assigned_count = int(assigned_count)
    count = int(count)
    return [prefix + str(x) for x in range(assigned_count + 1, assigned_count + count + 1)]


def write_data_to_files(schedule_4h, roster_4h, schedule_constrained, roster_constrained,
                        cluster, monday):
    opf = pd.ExcelWriter(output_path + cluster + '_' + monday.strftime('%Y%m%d') + '.xlsx')
    schedule_4h.to_excel(opf, 'schedule_4h', index=False, encoding='utf-8')
    roster_4h.to_excel(opf, 'roster_4h', index=False, encoding='utf-8')
    schedule_constrained.to_excel(opf, 'schedule_constrained', index=False, encoding='utf-8')
    roster_constrained.to_excel(opf, 'roster_constrained', index=False, encoding='utf-8')
    opf.save()


cnx = sqlcon.connect(user=db.USER, password=db.PWD,
                     host=db.HOST, database=db.DATABASE)

if __name__ == '__main__':
    main()
