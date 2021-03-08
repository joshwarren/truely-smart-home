# truely-smart-home
A system to optimise energy use in a domestic property. This project will initially be just for my needs, but may expand in the future to include other requirements e.g. other suppliers/PV systems/smart home devices.

Supported (planned) systems:
* Solax Inverter (with battery) on a PV solar pannel system
* Hot water tank/immersion, currently a conversional one using a Sonoff three way switch, but a modern one is set to be installed before the end of the year
* Samsung air source heat pump (installed at the same time as the new hot water tank. 
* Underfloor heating (using the heat pump) and radiator circuits.

Data sources:
* [Open Weather](https://openweathermap.org/api/one-call-api)
* [Octopus tariff API](https://developer.octopus.energy/docs/api/#agile-octopus)
* [Solax Inverter](https://www.solaxcloud.com/)
* Smart meter (installation is very soon)
* remote thermometers installed with underfloor heating

## Current state of play
A proof of concept has been developed to run in a Docker container on a Raspberry Pi 3. This collect daily data cuts from Open Weather and Octopus Energy, saving it to a Postgres database.

## Future plans
* Use data science techniques (e.g. machine learning) to investigate how we use energy and if any savings could be made.
* Automate switching systems on/off depending on need e.g. 
  * turning on heating when electricity is cheapest (or the pannels are producing electricity) if we are going to need the energy
  * Charging battery from the grid if Octopus tariffs turn negative i.e. they pay us to use electricity - it does happen [sometimes](https://octopus.energy/blog/social-distancing-renewable-energy-negative-pricing/)
* Produce a mobile app to control aspects of the home remotely
  * Extend this to use mobile as a data source - e.g. turn on heating when I'm 10 mins away from home. 

## Learning points
This is a list of parts of the project which I am actively learning good practise. If you have any suggests for how my code could be improved on these areas, I would be very happy to hear from you.
* Secret handling
* Docker (incl. Docker-compose)
* Python GUIs
* Networking
* Event-driven code
