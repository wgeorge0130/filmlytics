import pymongo
import csv
import json
import matplotlib.pyplot as plt
import numpy as np
from pymongo import MongoClient
from sklearn.linear_model import LinearRegression
import pygal
from pygal.style import Style
from countryCodes import countryCodes
from scipy.optimize import curve_fit


def insert(collection):
    with open('movies.csv', mode='r', encoding='utf-8') as csv_file:
        dict_objs = csv.DictReader(csv_file)
        id = 1
        for d in dict_objs:
            d["_id"] = id
            # json_obj = json.dumps(d)
            collection.insert_one(d)
            id += 1


"""
Use MongoDB's aggregration pipeline to compute the most popular/acclaimed movies based on genre.
Provide a data visualization indicating the disparity of genres among popular/acclaimed films.
"""


def topPopularGenres(collection, n):
    collection.update_many({"$or": [{"score": ""}, {"votes": ""}]}, {"$set": {"score": 0, "votes": 0}})
    doubleConversion = {
        "$addFields": {
            "convertedScore": {"$toDouble": "$score"},
            "convertedVotes": {"$toDouble": "$votes"},
        }
    }

    project = {
        "$project": {"_id": 0, "name": 1, "genre": 1, "metric": {"$multiply": ["$convertedScore", "$convertedVotes"]}},
    }

    sort = {
        "$sort": {"metric": -1}
    }

    limit = {"$limit": n}

    group = {"$group": {"_id": "$genre", "total_amount": {"$sum": 1}}}

    result = collection.aggregate(
        [
            doubleConversion,
            project,
            sort,
            limit,
            group
        ]
    )
    # cursor = collection.find().sort({"score": -1}).limit(15)

    genres = []
    totals = []

    for json in result:
        print(json)
        genres.append(json["_id"])
        totals.append(json["total_amount"])

    # Based on official documentation of matplotlib.
    figure = plt.figure(figsize=(20, 10))

    plt.bar(genres, totals, color='blue', width=0.4)

    plt.xlabel("Genres")
    plt.ylabel("Amount of movies")
    plt.title("Top Genres in Top 300 Most Popular Movies")
    plt.show()


"""
Utilize MongoDB querying to access various collections from the entire database of movie information. Top genre produced per country map

"""


def topGenreCountry(collection1):
    genresCountryMappings = {}

    results = []
    for key, val in countryCodes.items():
        match = {"$match": {"country": key}}
        doubleConversion = {
            "$addFields": {
                "convertedScore": {"$toDouble": "$score"},
                "convertedVotes": {"$toDouble": "$votes"},
            }
        }
        project = {
            "$project": {"_id": 0, "country": 1, "genre": 1,
                         "metric": {"$multiply": ["$convertedScore", "$convertedVotes"]}},
        }
        sort = {"$sort": {"metric": -1}}  # Descending order sort
        limit = {"$limit": 300}
        group = {"$group": {"_id": "$genre", "total_amount": {"$sum": 1}}}
        maximum1 = {"$sort": {"total_amount": -1}}
        maximum2 = {"$limit": 1}
        res = collection1.aggregate([match, doubleConversion, project, sort, limit, group, maximum1, maximum2])
        for json in res:
            if json["_id"] in genresCountryMappings:
                (genresCountryMappings[json["_id"]]).append(val)
            else:
                genresCountryMappings[json["_id"]] = [val]

    print(genresCountryMappings)
    wmap = pygal.maps.world.World()
    wmap.title = 'Top Genre Produced per Country'

    for key, val in genresCountryMappings.items():
        wmap.add(key, val)
    wmap.render_to_file("worldmap.svg")
    print("Map created in directory.")


# {'_id': 'Kenya', 'total_amount': 1}
# {'_id': 'Yugoslavia', 'total_amount': 5}
# {'_id': 'Spain', 'total_amount': 47}
# {'_id': 'Ireland', 'total_amount': 43}
# {'_id': 'Sweden', 'total_amount': 25}
# {'_id': 'Japan', 'total_amount': 80}
# {'_id': 'Iran', 'total_amount': 10}
# {'_id': 'Czech Republic', 'total_amount': 8}
# {'_id': 'France', 'total_amount': 279}
# {'_id': 'Greece', 'total_amount': 2}
# {'_id': 'Federal Republic of Yugoslavia', 'total_amount': 2}
# {'_id': 'Poland', 'total_amount': 4}
# {'_id': 'Serbia', 'total_amount': 1}
# {'_id': 'Libya', 'total_amount': 1}
# {'_id': 'West Germany', 'total_amount': 12}
# {'_id': 'Canada', 'total_amount': 190}
# {'_id': 'Italy', 'total_amount': 61}
# {'_id': 'Denmark', 'total_amount': 32}
# {'_id': 'Vietnam', 'total_amount': 2}
# {'_id': 'Iceland', 'total_amount': 2}
# {'_id': 'United Kingdom', 'total_amount': 816}
# {'_id': 'Colombia', 'total_amount': 1}
# {'_id': 'Turkey', 'total_amount': 3}
# {'_id': 'Jamaica', 'total_amount': 1}
# {'_id': 'Taiwan', 'total_amount': 7}
# {'_id': 'Malta', 'total_amount': 1}
# {'_id': 'United Arab Emirates', 'total_amount': 2}
# {'_id': 'Netherlands', 'total_amount': 12}
# {'_id': 'New Zealand', 'total_amount': 25}
# {'_id': 'China', 'total_amount': 40}
# {'_id': 'Romania', 'total_amount': 1}
# {'_id': 'Indonesia', 'total_amount': 2}
# {'_id': 'Thailand', 'total_amount': 6}
# {'_id': 'Philippines', 'total_amount': 3}
# {'_id': '', 'total_amount': 3}
# {'_id': 'India', 'total_amount': 62}
# {'_id': 'South Africa', 'total_amount': 8}
# {'_id': 'Israel', 'total_amount': 5}
# {'_id': 'Germany', 'total_amount': 117}
# {'_id': 'Portugal', 'total_amount': 2}
# {'_id': 'Australia', 'total_amount': 92}
# {'_id': 'Soviet Union', 'total_amount': 2}


"""
Utilize MongoDB querying to access various collections from the entire database of movie information.
Visualize the relationship between movie budget-revenue ratio and the country of the movie's production GDP. 
We also provide a model via simple linear regression.
"""


def budgetRevenueRelationship(collection1, collection2):
    # result = collection1.aggregate(
    #     [{"$group": {"_id": "$country", "total_amount": {"$sum": 1}}}]
    # )
    # for i in result:
    #     print(i)

    # string_conversion = {
    #     "$addFields": {
    #         "convertedBudget": {"$toString": "$budget"},
    #         "convertedGross": {"$toString": "gross"}
    #     }
    # }

    collection1.update_many({"$or": [{"score": 0}, {"gross": 0}, {"budget": 0}]},
                            {"$set": {"score": "", "budget": "", "gross": ""}})

    remove_whitespace = {
        "$addFields": {
            "country": {"$trim": {"input": "$Country"}}
        }
    }

    join = {
        "$lookup": {
            "from": "movies",
            "localField": "country",
            "foreignField": "country",
            "as": "MovieInfo"
        }
    }

    joined_res = collection2.aggregate([
        remove_whitespace,
        join,
    ])
    # Process data to be scattered/analyzed
    revGross = []
    gdps = []
    for x in joined_res:
        if x["MovieInfo"]:
            total = 0
            count = 0
            for y in x["MovieInfo"]:
                y["budget"] = str(y["budget"])
                y["gross"] = str(y["gross"])
                if (len(y["budget"]) != 0 and len(y["gross"]) != 0):
                    temp = float(y["budget"]) / float(y["gross"])
                    y["budgetGrossRatio"] = temp
                    total += temp
                    count += 1
                # print(y)
            if count != 0:
                avg = total / count
                revGross.append(avg)
                gdps.append(int(x["GDP ($ per capita)"]))

    figure = plt.figure(figsize=(15, 10))
    plt.scatter(revGross, gdps, color='green')

    plt.xlabel("BudgetGrossRatio")
    plt.ylabel("GDP ($ per capita)")
    plt.title("GDP vs Average Budget-Gross Ratio Per Country")

    # Best fit line for this data; However, not applicable for this specific data.
    #model = LinearRegression()
    #revGross = np.array(revGross)
    #gdps = np.array(gdps)
    #model.fit(revGross[:, np.newaxis], gdps)
    # print(model.coef_, model.intercept_)

    #plt.plot(revGross, model.predict(revGross[:, np.newaxis]), color='red')

    plt.show()


"""
Utilize MongoDB querying to access various collections from the entire database of movie information.
profit (gross - budget) vs score. Top directors based on said metric.
Top 10/20 directors in a bargraph, and the average metric is (profit x score), directors have to have atleast 2 films, 
"""


def profitScoreMetricAnalysis(collection1, n):
    collection1.update_many({"$or": [{"score": ""}, {"gross": ""}, {"budget": ""}]},
                            {"$set": {"score": 0, "budget": 0, "gross": 0}})
    doubleConversion = {
        "$addFields": {
            "convertedGross": {"$toDouble": "$gross"},
            "convertedBudget": {"$toDouble": "$budget"},
            "convertedScore": {"$toDouble": "$score"}
        }
    }

    project1 = {
        "$project": {"_id": 0, "convertedScore": 1, "director": 1,
                     "profit": {"$subtract": ["$convertedGross", "$convertedBudget"]}}
    }

    result = collection1.aggregate(
        [
            doubleConversion,
            project1
        ]
    )

    profit = []
    score = []

    for json in result:
        if json["profit"] > 0:
            profit.append(json["profit"])
            score.append(json["convertedScore"])

    def logfunc(x, a, b):
        return a * np.log(x) + b

    # Plot data analyzing profit and score; generate best fit curve for the data
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(15,15))
    axes[0].set_xlabel("Profit")
    axes[0].set_ylabel("Score")
    axes[0].set_title("Profit vs Score")
    score = np.array(score)
    popt, pcov = curve_fit(logfunc, profit, score)
    axes[0].scatter(profit, score, color='blue')
    axes[0].plot(profit, logfunc(profit, popt[0], popt[1]), color='red', label='Curve')
    # model.fit(profit[:,np.newaxis], score)
    # plt.plot(profit, model.predict(profit[:,np.newaxis]), color='red')

    project2 = {
        "$project": {"_id": 0, "convertedScore": 1, "director": 1,
                     "profit": 1, "metric":
                         {
                             "$cond": {"if": {"$ne": ["$profit", 0]},
                                       "then": {"$multiply": ["$profit", "$convertedScore"]}, "else": 0}
                         },
                     "count_movie":
                         {
                             "$cond": {"if": {"$eq": ["$profit", 0]},
                                       "then": 0, "else": 1}
                         },
                     }
    }
    group = {
        "$group": {"_id": "$director", "total_movies": {"$sum": "$count_movie"}, "total_metric": {"$sum": "$metric"}}}
    metric_avg = {"$project": {"_id": 1, "metric_avg":
        {
            "$cond": {"if": {"$lt": ["$total_movies", 2]}, "then": -999999999999999,
                      "else": {"$divide": ["$total_metric", "$total_movies"]}}
        }}}
    sort = {"$sort": {"metric_avg": -1}}
    limit = {"$limit": n}
    result = collection1.aggregate([
        doubleConversion,
        project1,
        project2,
        group,
        metric_avg,
        sort,
        limit
    ])

    directors = []
    metric_avgs = []
    for json in result:
        directors.append(json["_id"])
        metric_avgs.append(json["metric_avg"])

    axes[1].set_xlabel("Directors")
    axes[1].set_ylabel("Avg Profit-Score Metric")
    axes[1].set_title("Top Directors Based on Avg Profit-Score Metric")
    axes[1].bar(directors, metric_avgs, color='orange', width=0.4)
    plt.subplots_adjust(hspace=0.5)
    plt.show()

    


if __name__ == "__main__":
    client = pymongo.MongoClient(
        "mongodb+srv://george:sujoysikdar@cluster0.frmhm.mongodb.net/admin?retryWrites=true&w=majority")
    db = client['movie_information']
    collection1 = db["movies"]
    collection2 = db["countries"]

    while (1):
        print(
            "Select the following data analysis functionalities for our movies NOSQL database: \n 1: Compute the most popular/acclaimed movies based on genre with visualization \n 2: Show the top genre of movies produced per country to display on map \n 3: Analyze the relationship of GDP vs. the budget-gross ratio of movies per country \n 4: Analyze the relationship between profit vs. the score of a movie as well as this this profit-score metric to determine the top n directors\n q: Quit program")
        inp = input("Enter 1, 2, 3, 4, or q: ")
        if (inp == "1"):
            n = int(input("Set n value for limit (Distribution of genres of top-popular n movies): \n"))
            bool = n > 0 and n <= 6000
            while (not bool):
                n = int(input("Please input a valid input (n > 0 and n <= 60000): \n"))
            topPopularGenres(collection1, n)
        elif (inp == "2"):
            topGenreCountry(collection1)
        elif (inp == "3"):
            budgetRevenueRelationship(collection1, collection2)
        elif (inp == "4"):
            n = int(input("Set n value for limit (Top n directors based on profit-score metric): \n"))
            bool = n > 0 and n <= 10
            while (not bool):
                n = int(input("Please input a valid input (n > 0 and n <= 10): \n"))
            profitScoreMetricAnalysis(collection1, n)
        elif (inp == "q"):
            break
        else:
            print("Invalid input! Please type in either 1, 2, 3, 4, or q.")
