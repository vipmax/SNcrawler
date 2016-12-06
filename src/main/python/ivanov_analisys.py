import json
import numpy
import pymongo
from shapely.geometry import Polygon, Point

vk_posts_collection = pymongo.MongoClient(host="192.168.13.133")['Social']['Posts'].find()

spb_zone = Polygon([
    (60.100512, 30.079934),
    (60.096185, 30.564412),
    (59.808623, 30.529900),
    (59.808133, 30.057759),
])

counter = 0

points = []

for data in vk_posts_collection:
    # print(data)

    try:
        if data['network'] == "vkontakte":
            if 'geo' in data.keys():
                geo = data['geo']['coordinates'].split(' ')
                lat  = float(geo[0])
                long = float(geo[1])

        if data['network'] == "instagram":
            if 'location' in data.keys():
                geo = data['location']
                lat = float(geo['lat'])
                long = float(geo['lng'])

        if spb_zone.contains(Point(lat, long)):
            counter += 1
            points.append([lat, long])
            print(counter)
    except:
        pass

from sklearn.cluster import KMeans
import numpy as np

##############################################################################

X = numpy.array(points)

##############################################################################
# Compute clustering with Means

estimator = KMeans(init='k-means++', n_clusters=1000, n_init=10)
estimator.fit(X)

##############################################################################
# Plot the results
# for i in set(k_means.labels_):
#     index = k_means.labels_ == i
#     plt.plot(X[index,0], X[index,1], 'o')
# plt.show()
labels = estimator.labels_
centroids = estimator.cluster_centers_

data = {}
# json_data = json.dumps(data)


for cluster_id in range(estimator.n_clusters):
    cluster_centroid = centroids[cluster_id]
    cluster_points = X[np.where(labels == cluster_id)]

    data['cluster_centroid'] = cluster_centroid.tolist()
    data['cluster_points'] = cluster_points.tolist()
    data['cluster_points_count'] = len(cluster_points)

    # print(cluster_id, cluster_centroid, cluster_points)

    # print(cluster_id)
    # print('[{}, {}],'.format(cluster_centroid[0], cluster_centroid[1]))
    # for cluster_point in cluster_points:
    #     print('[{}, {}],'.format(cluster_point[0],cluster_point[1]))

    with open('all_clustered_data.json', 'a') as f:
        f.write('{} \n'.format(json.dumps(data)))


# numpy.savetxt("clusters.csv", estimator.cluster_centers_, delimiter=",", fmt='%10.12f')